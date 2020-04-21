/*****************************************************************************

Copyright (c) 1995, 2017, Oracle and/or its affiliates. All Rights Reserved.
Copyright (c) 2015, 2020, MariaDB Corporation.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin Street, Fifth Floor, Boston, MA 02110-1335 USA

*****************************************************************************/

/**************************************************//**
@file buf/buf0rea.cc
The database buffer read

Created 11/5/1995 Heikki Tuuri
*******************************************************/

#include "univ.i"
#include <mysql/service_thd_wait.h>

#include "buf0rea.h"
#include "fil0fil.h"
#include "mtr0mtr.h"
#include "buf0buf.h"
#include "buf0flu.h"
#include "buf0lru.h"
#include "buf0dblwr.h"
#include "ibuf0ibuf.h"
#include "log0recv.h"
#include "trx0sys.h"
#include "os0file.h"
#include "srv0start.h"
#include "srv0srv.h"

/** If there are buf_pool.curr_size per the number below pending reads, then
read-ahead is not done: this is to prevent flooding the buffer pool with
i/o-fixed buffer blocks */
#define BUF_READ_AHEAD_PEND_LIMIT	2

/********************************************************************//**
Unfixes the pages, unlatches the page,
removes it from page_hash and removes it from LRU. */
static
void
buf_read_page_handle_error(
/*=======================*/
	buf_page_t*	bpage)	/*!< in: pointer to the block */
{
	const bool	uncompressed = (buf_page_get_state(bpage)
					== BUF_BLOCK_FILE_PAGE);
	const page_id_t	old_page_id = bpage->id;

	/* First unfix and release lock on the bpage */
	mutex_enter(&buf_pool.mutex);
	mutex_enter(buf_page_get_mutex(bpage));
	ut_ad(buf_page_get_io_fix(bpage) == BUF_IO_READ);

	bpage->id.set_corrupt_id();
	/* Set BUF_IO_NONE before we remove the block from LRU list */
	buf_page_set_io_fix(bpage, BUF_IO_NONE);

	if (uncompressed) {
		rw_lock_x_unlock_gen(
			&((buf_block_t*) bpage)->lock,
			BUF_IO_READ);
	}

	mutex_exit(buf_page_get_mutex(bpage));

	/* remove the block from LRU list */
	buf_LRU_free_one_page(bpage, old_page_id);

	ut_ad(buf_pool.n_pend_reads > 0);
	buf_pool.n_pend_reads--;

	mutex_exit(&buf_pool.mutex);
}

/** Low-level function which reads a page asynchronously from a file to the
buffer buf_pool if it is not already there, in which case does nothing.
Sets the io_fix flag and sets an exclusive lock on the buffer frame. The
flag is cleared and the x-lock released by an i/o-handler thread.

@param[out] err		DB_SUCCESS or DB_TABLESPACE_DELETED
			if we are trying
			to read from a non-existent tablespace
@param[in] sync		true if synchronous aio is desired
@param[in] mode		BUF_READ_IBUF_PAGES_ONLY, ...,
@param[in] page_id	page id
@param[in] zip_size	ROW_FORMAT=COMPRESSED page size, or 0
@param[in] unzip	true=request uncompressed page
@param[in] ignore	whether to ignore out-of-bounds page_id
@return 1 if a read request was queued, 0 if the page already resided
in buf_pool, or if the page is in the doublewrite buffer blocks in
which case it is never read into the pool, or if the tablespace does
not exist or is being dropped */
static
ulint
buf_read_page_low(
	dberr_t*		err,
	bool			sync,
	ulint			mode,
	const page_id_t		page_id,
	ulint			zip_size,
	bool			unzip,
	bool			ignore = false)
{
	buf_page_t*	bpage;

	*err = DB_SUCCESS;

	if (!page_id.space() && buf_dblwr_page_inside(page_id.page_no())) {

		ib::error() << "Trying to read doublewrite buffer page "
			<< page_id;
		return(0);
	}

	if (ibuf_bitmap_page(page_id, zip_size) || trx_sys_hdr_page(page_id)) {

		/* Trx sys header is so low in the latching order that we play
		safe and do not leave the i/o-completion to an asynchronous
		i/o-thread. Ibuf bitmap pages must always be read with
		syncronous i/o, to make sure they do not get involved in
		thread deadlocks. */

		sync = true;
	}

	/* The following call will also check if the tablespace does not exist
	or is being dropped; if we succeed in initing the page in the buffer
	pool for read, then DISCARD cannot proceed until the read has
	completed */
	bpage = buf_page_init_for_read(err, mode, page_id, zip_size, unzip);

	if (bpage == NULL) {

		return(0);
	}

	DBUG_LOG("ib_buf",
		 "read page " << page_id << " zip_size=" << zip_size
		 << " unzip=" << unzip << ',' << (sync ? "sync" : "async"));

	ut_ad(buf_page_in_file(bpage));

	if (sync) {
		thd_wait_begin(NULL, THD_WAIT_DISKIO);
	}

	void*	dst;

	if (zip_size) {
		dst = bpage->zip.data;
	} else {
		ut_a(buf_page_get_state(bpage) == BUF_BLOCK_FILE_PAGE);

		dst = ((buf_block_t*) bpage)->frame;
	}

	fil_io_t fio = fil_io(
		IORequestRead, sync, page_id, zip_size, 0,
		zip_size ? zip_size : srv_page_size,
		dst, bpage, ignore);

	*err= fio.err;

	if (UNIV_UNLIKELY(fio.err != DB_SUCCESS)) {
		if (ignore || fio.err == DB_TABLESPACE_DELETED) {
			buf_read_page_handle_error(bpage);
			if (sync && fio.node) {
				fio.node->space->release_for_io();
			}
			return(0);
		}

		ut_error;
	}

	if (sync) {
		thd_wait_end(NULL);

		/* The i/o was already completed in fil_io() */
		*err = buf_page_read_complete(bpage, *fio.node);
		fio.node->space->release_for_io();

		if (*err != DB_SUCCESS) {
			return(0);
		}
	}

	return(1);
}

/** Applies a random read-ahead in buf_pool if there are at least a threshold
value of accessed pages from the random read-ahead area. Does not read any
page, not even the one at the position (space, offset), if the read-ahead
mechanism is not activated. NOTE 1: the calling thread may own latches on
pages: to avoid deadlocks this function must be written such that it cannot
end up waiting for these latches! NOTE 2: the calling thread must want
access to the page given: this rule is set to prevent unintended read-aheads
performed by ibuf routines, a situation which could result in a deadlock if
the OS does not support asynchronous i/o.
@param[in]	page_id		page id of a page which the current thread
wants to access
@param[in]	zip_size	ROW_FORMAT=COMPRESSED page size, or 0
@param[in]	ibuf		whether we are inside ibuf routine
@return number of page read requests issued; NOTE that if we read ibuf
pages, it may happen that the page at the given page number does not
get read even if we return a positive value! */
ulint
buf_read_ahead_random(const page_id_t page_id, ulint zip_size, bool ibuf)
{
  if (!srv_random_read_ahead)
    return 0;

  if (srv_startup_is_before_trx_rollback_phase)
    /* No read-ahead to avoid thread deadlocks */
    return 0;

  if (ibuf_bitmap_page(page_id, zip_size) || trx_sys_hdr_page(page_id))
    /* If it is an ibuf bitmap page or trx sys hdr, we do no
    read-ahead, as that could break the ibuf page access order */
    return 0;

  if (buf_pool.n_pend_reads > buf_pool.curr_size / BUF_READ_AHEAD_PEND_LIMIT)
    return 0;

  fil_space_t* space= fil_space_acquire(page_id.space());
  if (!space)
    return 0;

  const uint32_t buf_read_ahead_area= buf_pool.read_ahead_area;
  ulint count= 5 + buf_read_ahead_area / 8;
  const page_id_t low= page_id - (page_id.page_no() % buf_read_ahead_area);
  page_id_t high= low + buf_read_ahead_area;
  high.set_page_no(std::min(high.page_no(),
                            static_cast<uint32_t>(space->size - 1)));

  /* Count how many blocks in the area have been recently accessed,
  that is, reside near the start of the LRU list. */

  for (page_id_t i= low; i < high; ++i)
  {
    const ulint fold= i.fold();
    rw_lock_t *hash_lock= buf_pool.page_hash_lock_confirmed<false>(fold);
    const buf_page_t* bpage= buf_pool.page_hash_get_low(i);
    bool found= bpage && bpage->is_accessed() && buf_page_peek_if_young(bpage);
    rw_lock_s_unlock(hash_lock);
    if (found && !--count)
      goto read_ahead;
  }

  space->release();
  return 0;

read_ahead:
  /* Read all the suitable blocks within the area */
  const ulint ibuf_mode= ibuf ? BUF_READ_IBUF_PAGES_ONLY : BUF_READ_ANY_PAGE;

  for (page_id_t i= low; i < high; ++i)
  {
    if (ibuf_bitmap_page(i, zip_size))
      continue;
    dberr_t err;
    count+= buf_read_page_low(&err, false, ibuf_mode, i, zip_size, false);
  }

  if (count)
    DBUG_PRINT("ib_buf", ("random read-ahead %zu pages from %s: %u",
			  count, space->chain.start->name,
			  low.page_no()));
  space->release();

  /* Read ahead is considered one I/O operation for the purpose of
  LRU policy decision. */
  buf_LRU_stat_inc_io();

  buf_pool.stat.n_ra_pages_read_rnd+= count;
  srv_stats.buf_pool_reads.add(count);
  return count;
}

/** High-level function which reads a page asynchronously from a file to the
buffer buf_pool if it is not already there. Sets the io_fix flag and sets
an exclusive lock on the buffer frame. The flag is cleared and the x-lock
released by the i/o-handler thread.
@param[in]	page_id		page id
@param[in]	zip_size	ROW_FORMAT=COMPRESSED page size, or 0
@retval DB_SUCCESS if the page was read and is not corrupted,
@retval DB_PAGE_CORRUPTED if page based on checksum check is corrupted,
@retval DB_DECRYPTION_FAILED if page post encryption checksum matches but
after decryption normal page checksum does not match.
@retval DB_TABLESPACE_DELETED if tablespace .ibd file is missing */
dberr_t buf_read_page(const page_id_t page_id, ulint zip_size)
{
	ulint		count;
	dberr_t		err = DB_SUCCESS;

	/* We do synchronous IO because our AIO completion code
	is sub-optimal. See buf_page_read_complete(), we have to
	acquire the buffer pool mutex before acquiring the block
	mutex, required for updating the page state. The acquire
	of the buffer pool mutex becomes an expensive bottleneck. */

	count = buf_read_page_low(
		&err, true, BUF_READ_ANY_PAGE, page_id, zip_size, false);

	srv_stats.buf_pool_reads.add(count);

	if (err == DB_TABLESPACE_DELETED) {
		ib::info() << "trying to read page " << page_id
			<< " in nonexisting or being-dropped tablespace";
	}

	/* Increment number of I/O operations used for LRU policy. */
	buf_LRU_stat_inc_io();

	return(err);
}

/** High-level function which reads a page asynchronously from a file to the
buffer buf_pool if it is not already there. Sets the io_fix flag and sets
an exclusive lock on the buffer frame. The flag is cleared and the x-lock
released by the i/o-handler thread.
@param[in]	page_id		page id
@param[in]	zip_size	ROW_FORMAT=COMPRESSED page size, or 0
@param[in]	sync		true if synchronous aio is desired */
void
buf_read_page_background(const page_id_t page_id, ulint zip_size, bool sync)
{
	ulint		count;
	dberr_t		err;

	count = buf_read_page_low(
		&err, sync,
		BUF_READ_ANY_PAGE,
		page_id, zip_size, false, true);

	switch (err) {
	case DB_SUCCESS:
	case DB_ERROR:
		break;
	case DB_TABLESPACE_DELETED:
		ib::info() << "trying to read page " << page_id
			<< " in the background"
			" in a non-existing or being-dropped tablespace";
		break;
	case DB_PAGE_CORRUPTED:
	case DB_DECRYPTION_FAILED:
		ib::error()
			<< "Background Page read failed to "
			"read or decrypt " << page_id;
		break;
	default:
		ib::fatal() << "Error " << err << " in background read of "
			<< page_id;
	}

	srv_stats.buf_pool_reads.add(count);

	/* We do not increment number of I/O operations used for LRU policy
	here (buf_LRU_stat_inc_io()). We use this in heuristics to decide
	about evicting uncompressed version of compressed pages from the
	buffer pool. Since this function is called from buffer pool load
	these IOs are deliberate and are not part of normal workload we can
	ignore these in our heuristics. */
}

/** Applies linear read-ahead if in the buf_pool the page is a border page of
a linear read-ahead area and all the pages in the area have been accessed.
Does not read any page if the read-ahead mechanism is not activated. Note
that the algorithm looks at the 'natural' adjacent successor and
predecessor of the page, which on the leaf level of a B-tree are the next
and previous page in the chain of leaves. To know these, the page specified
in (space, offset) must already be present in the buf_pool. Thus, the
natural way to use this function is to call it when a page in the buf_pool
is accessed the first time, calling this function just after it has been
bufferfixed.
NOTE 1: as this function looks at the natural predecessor and successor
fields on the page, what happens, if these are not initialized to any
sensible value? No problem, before applying read-ahead we check that the
area to read is within the span of the space, if not, read-ahead is not
applied. An uninitialized value may result in a useless read operation, but
only very improbably.
NOTE 2: the calling thread may own latches on pages: to avoid deadlocks this
function must be written such that it cannot end up waiting for these
latches!
NOTE 3: the calling thread must want access to the page given: this rule is
set to prevent unintended read-aheads performed by ibuf routines, a situation
which could result in a deadlock if the OS does not support asynchronous io.
@param[in]	page_id		page id; see NOTE 3 above
@param[in]	zip_size	ROW_FORMAT=COMPRESSED page size, or 0
@param[in]	ibuf		whether if we are inside ibuf routine
@return number of page read requests issued */
ulint
buf_read_ahead_linear(const page_id_t page_id, ulint zip_size, bool ibuf)
{
  /* check if readahead is disabled */
  if (!srv_read_ahead_threshold)
    return 0;

  if (srv_startup_is_before_trx_rollback_phase)
    /* No read-ahead to avoid thread deadlocks */
    return 0;

  if (buf_pool.n_pend_reads > buf_pool.curr_size / BUF_READ_AHEAD_PEND_LIMIT)
    return 0;

  const uint32_t buf_read_ahead_area= buf_pool.read_ahead_area;
  const page_id_t low= page_id - (page_id.page_no() % buf_read_ahead_area);
  const page_id_t high_1= low + (buf_read_ahead_area - 1);

  /* We will check that almost all pages in the area have been accessed
  in the desired order. */
  const bool descending= page_id == low;

  if (!descending && page_id != high_1)
    /* This is not a border page of the area */
    return 0;

  if (ibuf_bitmap_page(page_id, zip_size) || trx_sys_hdr_page(page_id))
    /* If it is an ibuf bitmap page or trx sys hdr, we do no
    read-ahead, as that could break the ibuf page access order */
    return 0;

  fil_space_t *space= fil_space_acquire(page_id.space());
  if (!space)
    return 0;
  if (high_1.page_no() >= space->size)
  {
    /* The area is not whole. */
    space->release();
    return 0;
  }

  /* How many out of order accessed pages can we ignore
  when working out the access pattern for linear readahead */
  ulint count= std::min<ulint>(buf_pool_t::READ_AHEAD_PAGES -
                               srv_read_ahead_threshold,
                               uint32_t{buf_pool.read_ahead_area});
  page_id_t new_low= low, new_high_1= high_1;
  unsigned prev_accessed= 0;
  for (page_id_t i= low; i != high_1; ++i)
  {
    const ulint fold= i.fold();
    rw_lock_t *hash_lock= buf_pool.page_hash_lock_confirmed<false>(fold);
    const buf_page_t* bpage= buf_pool.page_hash_get_low(i);
    if (i == page_id)
    {
      /* Read the natural predecessor and successor page addresses from
      the page; NOTE that because the calling thread may have an x-latch
      on the page, we do not acquire an s-latch on the page, this is to
      prevent deadlocks. The hash_lock is only protecting the
      buf_pool.page_hash for page i, not the bpage contents itself. */
      if (!bpage)
      {
hard_fail:
        rw_lock_s_unlock(hash_lock);
        space->release();
        return 0;
      }
      const byte *f;
      switch (UNIV_EXPECT(bpage->state, BUF_BLOCK_FILE_PAGE)) {
      case BUF_BLOCK_FILE_PAGE:
        f= reinterpret_cast<const buf_block_t*>(bpage)->frame;
        break;
      case BUF_BLOCK_ZIP_PAGE:
      case BUF_BLOCK_ZIP_DIRTY:
        f= bpage->zip.data;
        break;
      default:
        goto hard_fail;
      }

      uint32_t prev= mach_read_from_4(my_assume_aligned<4>(f + FIL_PAGE_PREV));
      uint32_t next= mach_read_from_4(my_assume_aligned<4>(f + FIL_PAGE_NEXT));
      if (prev == FIL_NULL || next == FIL_NULL)
        goto hard_fail;
      page_id_t id= page_id;
      if (descending && next - 1 == page_id.page_no())
        id.set_page_no(prev);
      else if (!descending && prev + 1 == page_id.page_no())
        id.set_page_no(next);
      else
        goto hard_fail; /* Successor or predecessor not in the right order */

      new_low= id - (id.page_no() % buf_read_ahead_area);
      new_high_1= new_low + (buf_read_ahead_area - 1);

      if (id != new_low && id != new_high_1)
        /* This is not a border page of the area: return */
        goto hard_fail;
      if (new_high_1.page_no() >= space->size)
        /* The area is not whole */
        goto hard_fail;
    }
    else if (!bpage)
    {
failed:
      rw_lock_s_unlock(hash_lock);
      if (--count)
        continue;
      space->release();
      return 0;
    }

    const unsigned accessed= bpage->is_accessed();
    if (!accessed)
      goto failed;
    /* Note that buf_page_t::is_accessed() returns the time of the
    first access. If some blocks of the extent existed in the buffer
    pool at the time of a linear access pattern, the first access
    times may be nonmonotonic, even though the latest access times
    were linear. The threshold (srv_read_ahead_factor) should help a
    little against this. */
    bool fail= prev_accessed &&
      (descending ? prev_accessed > accessed : prev_accessed < accessed);
    prev_accessed= accessed;
    if (fail)
      goto failed;
    rw_lock_s_unlock(hash_lock);
  }

  /* If we got this far, read-ahead can be sensible: do it */
  count= 0;
  for (ulint ibuf_mode= ibuf ? BUF_READ_IBUF_PAGES_ONLY : BUF_READ_ANY_PAGE;
       new_low != new_high_1; ++new_low)
  {
    if (ibuf_bitmap_page(new_low, zip_size))
      continue;
    dberr_t err;
    count+= buf_read_page_low(&err, false, ibuf_mode, new_low, zip_size,
                              false);
  }

  if (count)
    DBUG_PRINT("ib_buf", ("random read-ahead %zu pages from %s: %u",
                          count, space->chain.start->name,
                          new_low.page_no()));
  space->release();

  /* Read ahead is considered one I/O operation for the purpose of
  LRU policy decision. */
  buf_LRU_stat_inc_io();

  buf_pool.stat.n_ra_pages_read+= count;
  return count;
}

/** Issues read requests for pages which recovery wants to read in.
@param[in]	sync		true if the caller wants this function to wait
for the highest address page to get read in, before this function returns
@param[in]	space_id	tablespace id
@param[in]	page_nos	array of page numbers to read, with the
highest page number the last in the array
@param[in]	n_stored	number of page numbers in the array */
void
buf_read_recv_pages(
	bool		sync,
	ulint		space_id,
	const ulint*	page_nos,
	ulint		n_stored)
{
	fil_space_t*		space	= fil_space_get(space_id);

	if (space == NULL) {
		/* The tablespace is missing: do nothing */
		return;
	}

	fil_space_open_if_needed(space);

	const ulint zip_size = space->zip_size();

	for (ulint i = 0; i < n_stored; i++) {
		const page_id_t	cur_page_id(space_id, page_nos[i]);

		ulint limit = 0;
		for (ulint j = 0; j < buf_pool.n_chunks; j++) {
			limit += buf_pool.chunks[j].size / 2;
		}

		for (ulint count = 0; buf_pool.n_pend_reads >= limit; ) {
			os_thread_sleep(10000);

			if (!(++count % 1000)) {

				ib::error()
					<< "Waited for " << count / 100
					<< " seconds for "
					<< buf_pool.n_pend_reads
					<< " pending reads";
			}
		}

		dberr_t err;
		buf_read_page_low(
			&err, sync && i + 1 == n_stored,
			BUF_READ_ANY_PAGE, cur_page_id, zip_size, true);

		if (err == DB_DECRYPTION_FAILED || err == DB_PAGE_CORRUPTED) {
			ib::error() << "Recovery failed to read or decrypt "
				<< cur_page_id;
		}
	}

	DBUG_PRINT("ib_buf", ("recovery read-ahead (%u pages)",
			      unsigned(n_stored)));
}

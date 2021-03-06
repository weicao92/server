/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
// vim: ft=cpp:expandtab:ts=8:sw=4:softtabstop=4:
#ident "$Id$"
/*======
This file is part of PerconaFT.


Copyright (c) 2006, 2015, Percona and/or its affiliates. All rights reserved.

    PerconaFT is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License, version 2,
    as published by the Free Software Foundation.

    PerconaFT is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with PerconaFT.  If not, see <http://www.gnu.org/licenses/>.

----------------------------------------

    PerconaFT is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License, version 3,
    as published by the Free Software Foundation.

    PerconaFT is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with PerconaFT.  If not, see <http://www.gnu.org/licenses/>.
======= */

#ident "Copyright (c) 2006, 2015, Percona and/or its affiliates. All rights reserved."

#include "test.h"

// verify that serializable cursor locks deleted keys so that  another transaction can not insert into the range being scanned by the cursor
// we create 2 level tree that looks like
// root node with pivot key 2
// left leaf contains keys 0, 1, and 2
// right leaf contains keys 3 and 4
// we delete key 2 while a snapshot txn exist so that garbage collection does not occur.
// txn_a walks a cursor through the deleted keys.
// when txn_a finishes reading the deleted keys, txn_b tries to insert key 2 and should get lock not granted.

#include <db.h>
#include <unistd.h>
#include <sys/stat.h>
#include <pthread.h>

static DB_ENV *env = NULL;
static DB_TXN *txn_a = NULL;
static DB_TXN *txn_b = NULL;
static DB *db = NULL;
static uint32_t db_page_size = 4096;
// static uint32_t db_basement_size = 4096;
static const char *envdir = TOKU_TEST_FILENAME;

static int 
my_compare(DB *this_db UU(), const DBT *a UU(), const DBT *b UU()) {
    assert(a->size == b->size);
    return memcmp(a->data, b->data, a->size);
}

static int 
my_generate_row(DB *dest_db UU(), DB *src_db UU(), DBT_ARRAY *dest_key_arrays UU(), DBT_ARRAY *dest_val_arrays UU(), const DBT *src_key UU(), const DBT *src_val UU()) {
    toku_dbt_array_resize(dest_key_arrays, 1);
    toku_dbt_array_resize(dest_val_arrays, 1);
    DBT *dest_key = &dest_key_arrays->dbts[0];
    DBT *dest_val = &dest_val_arrays->dbts[0];
    assert(dest_key->flags == DB_DBT_REALLOC);
    dest_key->data = toku_realloc(dest_key->data, src_key->size);
    memcpy(dest_key->data, src_key->data, src_key->size);
    dest_key->size = src_key->size;
    assert(dest_val->flags == DB_DBT_REALLOC);
    dest_val->data = toku_realloc(dest_val->data, src_val->size);
    memcpy(dest_val->data, src_val->data, src_val->size);
    dest_val->size = src_val->size;
    return 0;
}

static int
next_do_nothing(DBT const *UU(a), DBT  const *UU(b), void *UU(c)) {
    return 0;
}

static void *
do_insert_2(void *arg) {
    int r;
    uint64_t key = 2;
    char val[800]; memset(val, 0, sizeof val);
    DBT k,v;
    r = db->put(db, txn_b, dbt_init(&k, &key, sizeof key), dbt_init(&v, val, sizeof val), 0);
    assert(r == DB_LOCK_NOTGRANTED);
    return arg;
}

static ssize_t 
my_pread (int fd, void *buf, size_t count, off_t offset) {
    static int my_pread_count = 0;
    if (++my_pread_count == 5) {
        pthread_t id;
        pthread_create(&id, NULL, do_insert_2, NULL);
        void *ret;
        pthread_join(id, &ret);
    }
    return pread(fd, buf, count, offset);
}

static void 
run_test(void) {
    int r;
    r = db_env_create(&env, 0); CKERR(r);
    env->set_errfile(env, stderr);
    r = env->set_redzone(env, 0); CKERR(r);
    r = env->set_generate_row_callback_for_put(env, my_generate_row); CKERR(r);
    r = env->set_default_bt_compare(env, my_compare); CKERR(r);
    r = env->open(env, envdir, DB_INIT_LOCK|DB_INIT_LOG|DB_INIT_MPOOL|DB_INIT_TXN|DB_CREATE|DB_PRIVATE, S_IRWXU+S_IRWXG+S_IRWXO); CKERR(r);

    r = db_create(&db, env, 0); CKERR(r);
    r = db->set_pagesize(db, db_page_size);
    DB_TXN *txn = NULL;
    r = env->txn_begin(env, 0, &txn, 0); CKERR(r);
    r = db->open(db, txn, "foo.db", 0, DB_BTREE, DB_CREATE, S_IRWXU+S_IRWXG+S_IRWXO); CKERR(r);
    r = txn->commit(txn, 0);    CKERR(r);

    // build a tree with 2 leaf nodes
    r = env->txn_begin(env, 0, &txn, 0); CKERR(r);
    DB_LOADER *loader = NULL;
    r = env->create_loader(env, txn, &loader, db, 1, &db, NULL, NULL, 0); CKERR(r);
    for (uint64_t i = 0; i < 5; i++) {
        uint64_t key = i;
        char val[800]; memset(val, 0, sizeof val);
        DBT k,v;
        r = loader->put(loader, dbt_init(&k, &key, sizeof key), dbt_init(&v, val, sizeof val)); CKERR(r);
    }
    r = loader->close(loader); CKERR(r);
    r = txn->commit(txn, 0);    CKERR(r);

    // delete key 2
    r = env->txn_begin(env, 0, &txn, 0); CKERR(r);
    for (uint64_t i = 2; i < 3; i++) {
        uint64_t key = i;
        DBT k;
        r = db->del(db, txn, dbt_init(&k, &key, sizeof key), 0); CKERR(r);
    }
    r = txn->commit(txn, 0);    CKERR(r);

    // close and reopen
    r = db->close(db, 0);     CKERR(r);
    r = db_create(&db, env, 0); CKERR(r);
    r = env->txn_begin(env, 0, &txn, 0); CKERR(r);
    r = db->open(db, txn, "foo.db", 0, DB_BTREE, 0, S_IRWXU+S_IRWXG+S_IRWXO); CKERR(r);
    r = txn->commit(txn, 0);    CKERR(r);

    // create a txn that will try to insert key 2 while the serializable cursor is walking through the tree
    r = env->txn_begin(env, 0, &txn_b, 0); CKERR(r);

    // walk a serializable cursor through the tree
    r = env->txn_begin(env, 0, &txn_a, 0); CKERR(r);
    DBC *cursor = NULL;
    r = db->cursor(db, txn_a, &cursor, 0); CKERR(r);
    db_env_set_func_pread(my_pread);
    while (1) {
        r = cursor->c_getf_next(cursor, 0, next_do_nothing, NULL); 
        if (r != 0)
            break;
    }
    db_env_set_func_pread(NULL);
    r = cursor->c_close(cursor); CKERR(r);
    r = txn_a->commit(txn_a, 0);    CKERR(r);

    r = txn_b->commit(txn_b, 0); CKERR(r);

    r = db->close(db, 0);     CKERR(r);
    r = env->close(env, 0);   CKERR(r);
}

static int 
usage(void) {
    fprintf(stderr, "-v (verbose)\n");
    fprintf(stderr, "-q (quiet)\n");
    fprintf(stderr, "--envdir %s\n", envdir);
    return 1;
}

int
test_main (int argc , char * const argv[]) {
    for (int i = 1 ; i < argc; i++) {
        if (strcmp(argv[i], "-v") == 0 || strcmp(argv[i], "--verbose") == 0) {
            verbose++;
            continue;
        }
        if (strcmp(argv[i], "-q") == 0) {
            if (verbose > 0)
                verbose--;
            continue;
        }
        if (strcmp(argv[i], "--envdir") == 0 && i+1 < argc) {
            envdir = argv[++i];
            continue;
        }
        return usage();
    }

    char rmcmd[32 + strlen(envdir)]; 
    snprintf(rmcmd, sizeof rmcmd, "rm -rf %s", envdir);
    int r;
    r = system(rmcmd); CKERR(r);
    r = toku_os_mkdir(envdir, S_IRWXU+S_IRWXG+S_IRWXO);       CKERR(r);

    run_test();

    return 0;
}

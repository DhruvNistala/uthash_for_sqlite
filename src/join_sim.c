#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sqlite3.h>
#include <sys/time.h>
#include "uthash.h" // Include uthash for in-memory hash table

#ifndef JOIN_TYPE
#define JOIN_TYPE 0 // Default to Hash Join
#endif

int g_useTTJ = JOIN_TYPE; // Use compile-time flag for default join type

// Struct for a row in TableA
typedef struct {
    int key;
    char valA[128];
} RowA;

// Dynamic array for TableA
typedef struct {
    RowA *rows;
    size_t size;
    size_t capacity;
} TableAData;

// Hash table for TableB
typedef struct BEntry {
    int key;
    char valB[128];
    UT_hash_handle hh; // uthash handle
} BEntry;

// Join Operator Interface
typedef struct Operator Operator;

struct Operator {
    void *state;
    char *(*next)(Operator *op);
    void (*backjump)(Operator *op, void *parent, char *outerTuple);
};

// Join Operator state
typedef struct {
    Operator base;
    BEntry *bHash;
    TableAData *tableA;
    size_t outerPos;
    Operator *child;
    Operator *parent;
} JoinOperator;

/* -----------------------------------------------------
   (1) Dynamic TableA Handling
----------------------------------------------------- */
void initTableAData(TableAData *table, size_t initialCap) {
    table->rows = (RowA *)malloc(initialCap * sizeof(RowA));
    table->size = 0;
    table->capacity = initialCap;
}

void addRowA(TableAData *table, int key, const char *valA) {
    if (table->size >= table->capacity) {
        table->capacity *= 2;
        table->rows = (RowA *)realloc(table->rows, table->capacity * sizeof(RowA));
    }
    table->rows[table->size].key = key;
    strncpy(table->rows[table->size].valA, valA, 127);
    table->rows[table->size].valA[127] = '\0';
    table->size++;
}

/* -----------------------------------------------------
   (2) Helper Functions
----------------------------------------------------- */
char *do_join_concat(const char *valA, const char *valB) {
    char *joined = malloc(strlen(valA) + strlen(valB) + 64);
    sprintf(joined, "A.valA=%s | B.valB=%s", valA, valB);
    return joined;
}

/* -----------------------------------------------------
   (3) Hash Join next()
----------------------------------------------------- */
char *HJ_next(Operator *op) {
    JoinOperator *self = (JoinOperator *)op->state;

    while (self->outerPos < self->tableA->size) {
        int joinKey = self->tableA->rows[self->outerPos].key;
        const char *valA = self->tableA->rows[self->outerPos].valA;
        self->outerPos++;

        BEntry *found = NULL;
        HASH_FIND_INT(self->bHash, &joinKey, found);
        if (found) {
            return do_join_concat(valA, found->valB);
        }
    }
    return NULL; // No more rows
}

/* -----------------------------------------------------
   (4) Tree Tracker Join (TTJ) next()
----------------------------------------------------- */
char *TTJ_next(Operator *op) {
    JoinOperator *self = (JoinOperator *)op->state;

    while (self->outerPos < self->tableA->size) {
        int joinKey = self->tableA->rows[self->outerPos].key;
        const char *valA = self->tableA->rows[self->outerPos].valA;

        printf("[DEBUG] TTJ_next: Processing key=%d, valA=%s, outerPos=%zu\n", joinKey, valA, self->outerPos);

        // Move forward to avoid re-processing the same row infinitely
        self->outerPos++;

        // Lookup in bHash
        BEntry *found = NULL;
        HASH_FIND_INT(self->bHash, &joinKey, found);

        if (!found) {
            printf("[DEBUG] TTJ_next: No match for key=%d. Triggering backjump.\n", joinKey);
            op->backjump(op, self->parent, (char *)valA);
            continue;
        }

        // If there's a child operator, process child join
        if (self->child) {
            char *childResult = self->child->next(self->child);
            if (childResult) {
                char *joined = do_join_concat(valA, childResult);
                free(childResult);
                return joined;
            } else {
                printf("[DEBUG] TTJ_next: Child exhausted. Triggering backjump.\n");
                self->child->backjump(self->child, op, (char *)valA);
                continue;
            }
        } else {
            // No child, directly return the joined result
            char *joined = do_join_concat(valA, found->valB);
            printf("[DEBUG] TTJ_next: Returning result: %s\n", joined);
            return joined;
        }
    }

    printf("[DEBUG] TTJ_next: No more rows in TableA. Ending.\n");
    return NULL; // No more rows
}

/* -----------------------------------------------------
   (5) TTJ Backjump
----------------------------------------------------- */
void TTJ_backjump(Operator *op, void *parent, char *outerTuple) {
    JoinOperator *self = (JoinOperator *)op->state;

    if (self->parent == parent || parent == NULL) {
        if (self->outerPos > 0) {
            self->outerPos--;
            printf("[DEBUG] TTJ_backjump: Backtracked to outerPos=%zu for key=%s\n", self->outerPos, outerTuple);
        } else {
            // If we have no parent and outerPos is 0, skip row 0 and move on
            // or terminate if row 0 is invalid anyway
            printf("[DEBUG] TTJ_backjump: outerPos=0 and no parent -> skipping row or ending.\n");
            // Option 1: Skip this row entirely
            self->outerPos++;
        }
    } else if (self->parent) {
        printf("[DEBUG] TTJ_backjump: Propagating backjump to parent operator.\n");
        self->parent->backjump(self->parent, parent, outerTuple);
    }
}

/* -----------------------------------------------------
   (6) Create Operator
----------------------------------------------------- */
Operator *createJoinOperator(TableAData *tableA, BEntry *bHash, Operator *parent, Operator *child) {
    JoinOperator *jop = malloc(sizeof(JoinOperator));
    jop->bHash = bHash;
    jop->tableA = tableA;
    jop->outerPos = 0;
    jop->parent = parent;
    jop->child = child;

    Operator *op = malloc(sizeof(Operator));
    op->state = jop;

    if (g_useTTJ) {
        op->next = TTJ_next;
        op->backjump = TTJ_backjump;
    } else {
        op->next = HJ_next;
        op->backjump = NULL;
    }
    return op;
}

/* -----------------------------------------------------
   (7) Load Tables
----------------------------------------------------- */
void loadTables(sqlite3 *db, const char *tableName, TableAData *table, BEntry **bHash) {
    // Load TableA
    const char *sqlA = "SELECT key, valA FROM TableA;";
    sqlite3_stmt *stmtA;
    sqlite3_prepare_v2(db, sqlA, -1, &stmtA, NULL);
    initTableAData(table, 100);

    while (sqlite3_step(stmtA) == SQLITE_ROW) {
        int key = sqlite3_column_int(stmtA, 0);
        const char *valA = (const char *)sqlite3_column_text(stmtA, 1);
        if (!valA) valA = "";
        addRowA(table, key, valA);
    }
    sqlite3_finalize(stmtA);

    // Load TableB
    const char *sqlB = "SELECT key, valB FROM TableB;";
    sqlite3_stmt *stmtB;
    sqlite3_prepare_v2(db, sqlB, -1, &stmtB, NULL);

    while (sqlite3_step(stmtB) == SQLITE_ROW) {
        int key = sqlite3_column_int(stmtB, 0);
        const char *valB = (const char *)sqlite3_column_text(stmtB, 1);
        if (!valB) valB = "";

        BEntry *entry = malloc(sizeof(BEntry));
        entry->key = key;
        strncpy(entry->valB, valB, 127);
        entry->valB[127] = '\0';

        // If key already exists, replace or skip
        BEntry *found = NULL;
        HASH_FIND_INT(*bHash, &key, found);
        if (found) {
            HASH_DEL(*bHash, found);
            free(found);
        }

        HASH_ADD_INT(*bHash, key, entry);
    }
    sqlite3_finalize(stmtB);
}

/* -----------------------------------------------------
   (8) Main
----------------------------------------------------- */
int main(int argc, char **argv) {
    sqlite3 *db;
    if (sqlite3_open("test_db.db", &db) != SQLITE_OK) {
        fprintf(stderr, "Cannot open database: %s\n", sqlite3_errmsg(db));
        return 1;
    }

    TableAData tableA;
    BEntry *bHash = NULL;

    loadTables(db, "TableA", &tableA, &bHash);

    Operator *joinOp = createJoinOperator(&tableA, bHash, NULL, NULL);

    printf("---- Join Results ----\n");
    char *result;
    while ((result = joinOp->next(joinOp)) != NULL) {
        printf("%s\n", result);
        free(result);
    }

    // Cleanup
    free(tableA.rows);
    BEntry *curr, *tmp;
    HASH_ITER(hh, bHash, curr, tmp) {
        HASH_DEL(bHash, curr);
        free(curr);
    }

    sqlite3_close(db);
    return 0;
}
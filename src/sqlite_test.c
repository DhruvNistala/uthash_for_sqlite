#include <stdio.h>
#include <sqlite3.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define DATABASE_NAME "test_db.db"
#define NUM_JOIN_DEPTHS 8  // Join depth from 10 to 300 (increment by 10)
#define NUM_TESTS_PER_JOIN_DEPTH 1
#define NUM_COLUMNS 4  // 3 INT, 3 CHAR, 3 STRING, 3 FLOAT columns
#define NUM_ROWS 50

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sqlite3.h>
#include <sys/time.h>
#include "uthash.h"
/* uthash single-file library */

/* Global variable toggling the join approach:
 * 0 => Normal Hash Join
 * 1 => TTJ
 */
#ifndef JOIN_TYPE
#define JOIN_TYPE 0 // Default to Hash Join
#endif

int g_useTTJ = JOIN_TYPE; // Use compile-time flag for default join type




// Function to calculate elapsed time in microseconds
long getElapsedTimeInMicroseconds(struct timeval start, struct timeval end)
{
    return (end.tv_sec - start.tv_sec) * 1000000L + (end.tv_usec - start.tv_usec);
}

/*
   run_query: Times the entire SQL query execution in SQLite,
   printing the result rows and the total time of the query.
*/
long run_query(sqlite3 *db, const char *query, FILE* logfile)
{
    sqlite3_stmt *stmt;
    int rc = sqlite3_prepare_v2(db, query, -1, &stmt, NULL);

    if (rc != SQLITE_OK)
    {
        fprintf(stderr, "Failed to prepare statement: %s\n", sqlite3_errmsg(db));
        return -1;
    }

    printf("\nExecuting query:\n%s\n\n", query);

    struct timeval start, end;
    gettimeofday(&start, NULL);

    printf("Results:\n");
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW)
    {
    }

    gettimeofday(&end, NULL);
    long time_taken = getElapsedTimeInMicroseconds(start, end);
    printf("Time taken for query: %ld microseconds (%.6f seconds)\n", time_taken, time_taken / 1000000.0);

    if (rc != SQLITE_DONE)
    {
        fprintf(stderr, "Error ex ecuting query: %s\n", sqlite3_errmsg(db)); 
    }
    sqlite3_finalize(stmt);
    return time_taken;
}





/* ------------------------------------------------------------------
   TableA in-memory data structures & functions
------------------------------------------------------------------ */
typedef struct
{
    int key;
    char valA[128];
} RowA;

typedef struct
{
    RowA *rows;
    size_t size;
    size_t capacity;
} TableAData;

void initTableAData(TableAData *table, size_t initialCap)
{
    table->rows = (RowA *)malloc(initialCap * sizeof(RowA));
    table->size = 0;
    table->capacity = initialCap;
}

void addRowA(TableAData *table, int key, const char *valA)
{
    if (table->size >= table->capacity)
    {
        table->capacity *= 2;
        table->rows = (RowA *)realloc(table->rows, table->capacity * sizeof(RowA));
    }
    table->rows[table->size].key = key;
    strncpy(table->rows[table->size].valA, valA, 127);
    table->rows[table->size].valA[127] = '\0';
    table->size++;
}

/*
   TableB in-memory hash using uthash:
   Key: int  -> BEntry
*/
typedef struct BEntry
{
    int key;
    char valB[128];
    UT_hash_handle hh; // uthash handle
} BEntry;

/* =========================
   Operator Interface
   ========================= */
typedef struct Operator Operator; // forward declare

/* The “operator” interface has a next() call
   returning the next joined row, and a backjump()
   for TTJ logic. */
struct Operator
{
    void *state;
    char *(*next)(Operator *op);
    void (*backjump)(Operator *op, void *parent, char *outerTuple);
};

/* Minimal function for normal join: backjump no-op */
void HJ_backjump_noop(Operator *op, void *parent, char *outerTuple)
{
    (void)op;
    (void)parent;
    (void)outerTuple;
    // do nothing in normal hash join
}

/*
   We define an operator struct for the in-memory join:
   - references TableA as the "outer" data
   - references a hash table for TableB as "inner"
   - a current pointer etc.
*/
typedef struct
{
    Operator base;      // Operator interface
    BEntry *bHash;      // Hash table for B
    TableAData *tableA; // pointer to TableA data
    size_t outerPos;    // index for scanning TableA
    // for TTJ, we might store more info, e.g. parent pointer, etc.
    void *parent;
} JoinOperator;

/* Helper function to do a naive string concat for the join result */
char *do_join_concat(const char *valA, const char *valB)
{
    char *joined = malloc(strlen(valA) + strlen(valB) + 64);
    sprintf(joined, "A.valA=%s | B.valB=%s", valA, valB);
    return joined;
}

/* Normal Hash Join next() logic */
char *HJ_next(Operator *op)
{
    JoinOperator *self = (JoinOperator *)op->state;

    while (self->outerPos < self->tableA->size)
    {
        int joinKey = self->tableA->rows[self->outerPos].key;
        const char *valA = self->tableA->rows[self->outerPos].valA;
        self->outerPos++;

        // find match in bHash
        BEntry *found = NULL;
        HASH_FIND_INT(self->bHash, &joinKey, found);
        if (found)
        {
            // produce a joined row
            char *joined = do_join_concat(valA, found->valB);
            return joined;
        }
        // if not found => continue to next A row
    }
    return NULL; // no more
}

/* TTJ next() logic, referencing pseudocode:

   def ttj(t, plan, i):
     if i == plan.len(): print(t)
     else:
       R = plan[i]; P = parent(i, plan)
       if R[t] is None & P is not None:
         return BackJump(P)
       for m in R[t]:
         match ttj(t++m, plan, i+1)
         case BackJump(R): R[t].delete(m)
         case BackJump(x): return BackJump(x)
*/
char *TTJ_next(Operator *op)
{
    JoinOperator *self = (JoinOperator *)op->state;

    while (self->outerPos < self->tableA->size)
    {
        int joinKey = self->tableA->rows[self->outerPos].key;
        const char *valA = self->tableA->rows[self->outerPos].valA;
        self->outerPos++;

        // Find match in bHash
        BEntry *found = NULL;
        HASH_FIND_INT(self->bHash, &joinKey, found);
        if (!found)
        {
            // Trigger backjump
            op->backjump(op, self->parent, (char *)valA /*some representation*/);
            // then keep going
            continue;
        }
        else
        {
            // produce a joined row
            char *joined = do_join_concat(valA, found->valB);
            return joined;
        }
    }
    return NULL; // no more
}

/* TTJ backjump: if we are the guilty relation => remove from bHash.
   For demonstration, we assume that the parent's the guilty side = bHash
   if (self->parent == parent)
*/
void TTJ_backjump(Operator *op, void *parent, char *outerTuple)
{
    JoinOperator *self = (JoinOperator *)op->state;
    if (self->parent == parent)
    {
        // “remove offending tuple from the hash table”
        // But we only have valA? Actually we need the key to do so.
        // For demonstration, we can do nothing or store a mapping from valA->key if needed.
        // In a real system, we'd pass the key or the outer tuple object with the needed info.
        // Skipping implementation detail here.
    }
    else
    {
        // pass it upwards? if there's an outer operator above?
        // This code is a placeholder.
    }
}

/*
   Creates a join operator that uses either Normal HJ or TTJ
   depending on global g_useTTJ.
*/
Operator *createJoinOperator(TableAData *tableA, BEntry *bHash, void *parent)
{
    JoinOperator *jop = malloc(sizeof(JoinOperator));
    memset(jop, 0, sizeof(JoinOperator));
    jop->bHash = bHash;
    jop->tableA = tableA;
    jop->outerPos = 0;
    jop->parent = parent;

    Operator *op = malloc(sizeof(Operator));
    memset(op, 0, sizeof(Operator));
    op->state = jop;

    if (g_useTTJ == 1)
    {
        op->next = TTJ_next;
        op->backjump = TTJ_backjump;
    }
    else
    {
        op->next = HJ_next;
        op->backjump = HJ_backjump_noop;
    }
    return op;
}




// %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% END TTJ CODE %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

// List of column names to join on
char* column_names[NUM_COLUMNS];

// create a method to dynamically generate column names
// based on the number of columns. store the column names
// in an array called const char* column_names
void generate_column_names() {
    for (int i = 0; i < NUM_COLUMNS; i++) {
        char column_name[32];
        snprintf(column_name, sizeof(column_name), "column%d", i + 1);
        // round robin the column names between the 4 types
        if (i % 4 == 1) {
            strncat(column_name, "_char", sizeof(column_name) - strlen(column_name) - 1);
        } else if (i % 4 == 2) {
            strncat(column_name, "_str", sizeof(column_name) - strlen(column_name) - 1);
        } else if (i % 4 == 3) {
            strncat(column_name, "_float", sizeof(column_name) - strlen(column_name) - 1);
        } else {
            strncat(column_name, "_int", sizeof(column_name) - strlen(column_name) - 1);
        }
    
        // Dynamically allocate memory for the string and store it
        column_names[i] = malloc(strlen(column_name) + 1);  // +1 for null terminator
        if (column_names[i] != NULL) {
            strcpy(column_names[i], column_name);
        }
    }

    // Print the column names to the screen
    for (int i = 0; i < NUM_COLUMNS; i++) {
        printf("Column %d: %s\n", i + 1, column_names[i]);
    }
}


void create_times_table(sqlite3 *db) {
    const char *drop_table_sql = "DROP TABLE IF EXISTS query_times;";
    const char *create_table_sql = 
        "CREATE TABLE IF NOT EXISTS query_times ("
        "join_depth INTEGER, "
        "column_name TEXT, "
        "average_time REAL, "
        "query_text TEXT"
        ");";
    
    char *err_msg = NULL;
    if (sqlite3_exec(db, drop_table_sql, 0, 0, &err_msg) != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n", err_msg);
        sqlite3_free(err_msg);
        return;
    }

    if (sqlite3_exec(db, create_table_sql, 0, 0, &err_msg) != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n", err_msg);
        sqlite3_free(err_msg);
    }
}


// Function to insert the average time into the query_times table
void store_query_time(sqlite3 *db, int join_depth, const char *column_name, double average_time, const char *query) {
    const char *insert_query = "INSERT INTO query_times (join_depth, column_name, average_time, query_text) VALUES (?, ?, ?, ?);";
    
    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(db, insert_query, -1, &stmt, 0) != SQLITE_OK) {
        fprintf(stderr, "Failed to prepare statement: %s\n", sqlite3_errmsg(db));
        return;
    }

    sqlite3_bind_int(stmt, 1, join_depth);
    sqlite3_bind_text(stmt, 2, column_name, -1, SQLITE_STATIC);
    sqlite3_bind_double(stmt, 3, average_time);
    sqlite3_bind_text(stmt, 4, query, -1, SQLITE_STATIC);

    if (sqlite3_step(stmt) != SQLITE_DONE) {
        fprintf(stderr, "Failed to insert data into query_times table: %s\n", sqlite3_errmsg(db));
    }

    sqlite3_finalize(stmt);
}

// Function to print the query_times table to a new file
void print_query_times_to_file(sqlite3 *db) {
    FILE *output_file = fopen("query_times_output.txt", "w");
    if (!output_file) {
        fprintf(stderr, "Could not open file for writing.\n");
        return;
    }

    // Query to select all data from the query_times table
    const char *select_query = "SELECT join_depth, column_name, average_time, query_text FROM query_times;";
    
    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(db, select_query, -1, &stmt, 0) != SQLITE_OK) {
        fprintf(stderr, "Failed to prepare statement: %s\n", sqlite3_errmsg(db));
        fclose(output_file);
        return;
    }

    // Write the column headers to the output file
    fprintf(output_file, "Join Depth | Column Name | Average Time | Query\n");
    fprintf(output_file, "------------------------------------------------------------\n");

    // Execute the query and print each result to the output file
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        int join_depth = sqlite3_column_int(stmt, 0);
        const char *column_name = (const char *)sqlite3_column_text(stmt, 1);
        double average_time = sqlite3_column_double(stmt, 2);
        const char *query_text = (const char *)sqlite3_column_text(stmt, 3);

        // Write each row's data to the file
        fprintf(output_file, "%d | %s | %.6f | %s\n", join_depth, column_name, average_time, query_text);
    }

    // Clean up
    sqlite3_finalize(stmt);
    fclose(output_file);

    printf("Query times have been printed to query_times_output.txt.\n");
}

// Function to create a single table with the new schema
void create_table(sqlite3 *db, int table_num) {
    char query[1024];
    
    // Drop the table if it already exists
    snprintf(query, sizeof(query), "DROP TABLE IF EXISTS table%d;", table_num);
    char *err_msg = NULL;
    if (sqlite3_exec(db, query, 0, 0, &err_msg) != SQLITE_OK) {
        fprintf(stderr, "SQL error (DROP TABLE): %s\n", err_msg);
        sqlite3_free(err_msg);
        return;
    }

    // Now create the table with the new schema
    snprintf(query, sizeof(query), "CREATE TABLE table%d (", table_num);

    // create the table from the column_names array
    for (int i = 0; i < NUM_COLUMNS; i++) {
        if (i > 0) {
            strncat(query, ", ", sizeof(query) - strlen(query) - 1);
        }
        snprintf(query + strlen(query), sizeof(query) - strlen(query), "%s ", column_names[i]);
        
        // Add the data type for each column
        if (i % 4 == 0) {
            strncat(query, "INT", sizeof(query) - strlen(query) - 1);
        } else if (i % 4 == 1) {
            strncat(query, "CHAR(1)", sizeof(query) - strlen(query) - 1);
        } else if (i % 4 == 2) {
            strncat(query, "TEXT", sizeof(query) - strlen(query) - 1);
        } else {
            strncat(query, "REAL", sizeof(query) - strlen(query) - 1);
        }
    }

    strncat(query, ");", sizeof(query) - strlen(query) - 1);

    if (sqlite3_exec(db, query, 0, 0, &err_msg) != SQLITE_OK) {
        fprintf(stderr, "SQL error (CREATE TABLE): %s\n", err_msg);
        sqlite3_free(err_msg);
        return;
    }

    printf("Table table%d created.\n", table_num); // Print when the table is created
}


// Function to insert data into a table with randomized values
void insert_data(sqlite3 *db, int table_num) {
    for (int i = 0; i < NUM_ROWS; i++) {
        char query[1024];
        snprintf(query, sizeof(query), "INSERT INTO table%d VALUES(", table_num);
        

        int n = 1000;
        // generate n random strings
        char *strings[n];
        for (int i = 0; i < n; i++) {
            char *str = malloc(10);
            // the string should be test + i
            snprintf(str, 10, "test%d", i);
            str[9] = '\0';
            strings[i] = str;
        }

        // Insert random values for each column
        for (int j = 0; j < NUM_COLUMNS; j++) {
            if (j > 0) {
                strncat(query, ", ", sizeof(query) - strlen(query) - 1);
            }
            if (j % 4 == 0) {
                // choose a random number between 1 and 2 * n
                snprintf(query + strlen(query), sizeof(query) - strlen(query), "%d", rand() % (2 * n) + 1);
            } else if (j % 4 == 1) {
                snprintf(query + strlen(query), sizeof(query) - strlen(query), "'%c'", (char)(rand() % 26 + 'A'));
            } else if (j % 4 == 2) {
                snprintf(query + strlen(query), sizeof(query) - strlen(query), "'%s'", strings[rand() % n]);
            } else {
                snprintf(query + strlen(query), sizeof(query) - strlen(query), "%f", (float)(rand() % 100) / 10);
            }
        }

        strncat(query, ");", sizeof(query) - strlen(query) - 1);

        char *err_msg = NULL;
        if (sqlite3_exec(db, query, 0, 0, &err_msg) != SQLITE_OK) {
            fprintf(stderr, "SQL error: %s\n", err_msg);
            sqlite3_free(err_msg);
        }
    }
    printf("Data inserted into table%d.\n", table_num);  // Print when data is inserted
}

// Function to create SQL query with joins on a specific column
void create_query(int join_depth, const char* join_column, char *query) {
    // snprintf(query, 4096, "SELECT * FROM table1");

    // for (int i = 2; i <= join_depth; i++) {
    //     snprintf(query + strlen(query), 4096 - strlen(query), " LEFT JOIN table%d ON table1.%s = table%d.%s", i, join_column, i, join_column);
    // }

    snprintf(query, 4096, "SELECT * ");

    // Specify the FROM clause
    snprintf(query + strlen(query), 4096 - strlen(query), " FROM ");

    // add all the tables to the FROM clause
    for (int i = 1; i <= join_depth; i++) {
        if (i > 1) {
            snprintf(query + strlen(query), 4096 - strlen(query), ", ");
        }
        snprintf(query + strlen(query), 4096 - strlen(query), "table%d", i);
    }

    // Add the WHERE clause with conditions for joins
    for (int i = 2; i <= join_depth; i++) {
        if (i == 2) {
            // Start the WHERE clause
            snprintf(query + strlen(query), 4096 - strlen(query), " WHERE table1.%s = table%d.%s", join_column, i, join_column);
        } else {
            // Add subsequent conditions with AND
            snprintf(query + strlen(query), 4096 - strlen(query), " AND table%d.%s = table%d.%s", i - 1, join_column, i, join_column);
        }
    }

     // set query to this SELECT t1.column1, t2.column2, t1.column3, t2.column4 FROM table1 t1 JOIN table2 t2 ON t1.column1 = t2.column1 WHERE t1.column1 BETWEEN 500 AND 1500 AND t2.column3 LIKE 'test%';
    // snprintf(query, 4096, "SELECT t1.%s, t2.%s, t1.%s, t2.%s FROM table1 t1 JOIN table2 t2 ON t1.%s = t2.%s WHERE t1.%s BETWEEN 500 AND 1500 AND t2.%s LIKE 'test%%';", column_names[0], column_names[1], column_names[2], column_names[3], column_names[0], column_names[0], column_names[0], column_names[2]);
}

// Function to execute a query and measure time
double execute_query(sqlite3 *db, const char *query, FILE *logfile) {
    clock_t start_time = clock();
    
    char *err_msg = NULL;
    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(db, query, -1, &stmt, 0) != SQLITE_OK) {
        fprintf(stderr, "Failed to prepare statement: %s\n", sqlite3_errmsg(db));
        return -1;
    }

    while (sqlite3_step(stmt) != SQLITE_DONE) {
        // Do nothing, we just want to execute the query
    }

    clock_t end_time = clock();
    double time_taken = (double)(end_time - start_time) / CLOCKS_PER_SEC;
    
    // Log the query and time to file
    fprintf(logfile, "Query: %s\n", query);
    fprintf(logfile, "Execution Time: %f seconds\n", time_taken);

    // printf("Query executed in %f seconds.\n", time_taken);  // Print the execution time to the screen
    return time_taken;
}

// Function to test queries and log times
void test_queries(sqlite3 *db) {
    FILE *logfile = fopen("queries_and_times.txt", "w");
    if (!logfile) {
        fprintf(stderr, "Could not open file for writing.\n");
        return;
    }
    
    for (int join_depth = 2; join_depth <= NUM_JOIN_DEPTHS; join_depth += 1) {
        printf("Running tests for join depth %d...\n", join_depth);
    
        // Run 12 tests for each join depth (one for each column)
        for (int column_idx = 0; column_idx < NUM_COLUMNS; column_idx++) {

            // dont run tests for char columns
            if (column_idx % 4 == 1) {
                continue;
            }


            long total_time = 0;
            const char* join_column = column_names[column_idx];
            
            printf("Testing join column: %s\n", join_column);
            fprintf(logfile, "Testing join column: %s\n", join_column);
            char query[4096];  // Large buffer for the query

            // Run NUM_TESTS_PER_JOIN_DEPTH tests for average purposes
            for (int test_num = 0; test_num < NUM_TESTS_PER_JOIN_DEPTH; test_num++) {
                create_query(join_depth, join_column, query);  // Generate the query with the given join depth and column
                
                // Log the query being tested
                fprintf(logfile, "Test %d with join depth %d, column %s:\n", test_num + 1, join_depth, join_column);
                fprintf(logfile, "Query: %s\n", query);
                
                // Execute the query and measure the time taken
                total_time += run_query(db, query, logfile);
            }

            // Calculate and print the average time for this column and join depth
            double average_time = total_time / NUM_TESTS_PER_JOIN_DEPTH;
                
            // Insert the average time into the query_times table
            store_query_time(db, join_depth, join_column, (double) average_time, query);

            printf("Average time for join depth %d, column %s: %f seconds.\n------------------------------------------\n\n", join_depth, join_column, average_time);
            fprintf(logfile, "Average execution time for join depth %d, column %s: %d seconds.\n------------------------------------------\n\n", join_depth, join_column, average_time);
        }
    }

    fclose(logfile);  // Close the log file after finishing all tests
}

int main() {
    sqlite3 *db;
    if (sqlite3_open(DATABASE_NAME, &db)) {
        fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
        return 1;
    }

    generate_column_names();

    create_times_table (db);
    
    // Create tables and insert data
    for (int i = 1; i <= NUM_JOIN_DEPTHS; i++) {
        create_table(db, i);
        insert_data(db, i);
    }
    
    // Run the query tests
    test_queries(db);
    
    print_query_times_to_file(db);

    sqlite3_close(db);
    return 0;
}

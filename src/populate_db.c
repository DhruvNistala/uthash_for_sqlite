#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sqlite3.h>
#include <time.h>

#define DB_NAME "test_db.db"
#define TABLE_A_SIZE 100 // Number of rows in TableA
#define TABLE_B_SIZE 50  // Number of rows in TableB

// Function to execute SQL commands
void exec_sql(sqlite3 *db, const char *sql)
{
    char *errMsg = NULL;
    int rc = sqlite3_exec(db, sql, NULL, NULL, &errMsg);
    if (rc != SQLITE_OK)
    {
        fprintf(stderr, "Error executing SQL: %s\n", errMsg);
        sqlite3_free(errMsg);
    }
}

// Generate a random string of given length
void generate_random_string(char *str, int length)
{
    static const char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    for (int i = 0; i < length; i++)
    {
        str[i] = charset[rand() % (sizeof(charset) - 1)];
    }
    str[length] = '\0';
}

// Populate TableA with random values
void populate_table_a(sqlite3 *db, int size)
{
    printf("[INFO] Populating TableA with %d rows...\n", size);

    char sql[256];
    for (int i = 0; i < size; i++)
    {
        int key = rand() % (size / 2); // Random key with some overlap
        char valA[16];
        generate_random_string(valA, 10);

        snprintf(sql, sizeof(sql), "INSERT INTO TableA (key, valA) VALUES (%d, '%s');", key, valA);
        exec_sql(db, sql);
    }
}

// Populate TableB with random values
void populate_table_b(sqlite3 *db, int size)
{
    printf("[INFO] Populating TableB with %d rows...\n", size);

    char sql[256];
    for (int i = 0; i < size; i++)
    {
        int key = rand() % (size / 2); // Random key with some overlap
        char valB[16];
        generate_random_string(valB, 10);

        snprintf(sql, sizeof(sql), "INSERT INTO TableB (key, valB) VALUES (%d, '%s');", key, valB);
        exec_sql(db, sql);
    }
}

// Populate advanced tables with predefined data
void populate_advanced_tables(sqlite3 *db)
{
    printf("[INFO] Populating advanced tables...\n");

    const char *advanced_sql =
        "DROP TABLE IF EXISTS company_type;"
        "DROP TABLE IF EXISTS info_type;"
        "DROP TABLE IF EXISTS movie_companies;"
        "DROP TABLE IF EXISTS movie_info;"
        "DROP TABLE IF EXISTS title;"
        "CREATE TABLE company_type (id INTEGER PRIMARY KEY, kind TEXT);"
        "CREATE TABLE info_type (id INTEGER PRIMARY KEY, info TEXT);"
        "CREATE TABLE movie_companies (id INTEGER PRIMARY KEY, movie_id INTEGER, company_type_id INTEGER, note TEXT);"
        "CREATE TABLE movie_info (id INTEGER PRIMARY KEY, movie_id INTEGER, info_type_id INTEGER, note TEXT);"
        "CREATE TABLE title (id INTEGER PRIMARY KEY, title TEXT, production_year INTEGER);"

        // Populate company_type
        "INSERT INTO company_type (id, kind) VALUES (1, 'production companies');"

        // Populate info_type
        "INSERT INTO info_type (id, info) VALUES (1, 'top 250 rank'), (2, 'bottom 10 rank');"

        // Populate title
        "INSERT INTO title (id, title, production_year) VALUES "
        "(1, 'Sherlock Holmes', 2009), "
        "(2, 'Iron Man', 2008), "
        "(3, 'The Avengers', 2012);"

        // Populate movie_companies
        "INSERT INTO movie_companies (id, movie_id, company_type_id, note) VALUES "
        "(1, 1, 1, 'presents'), "
        "(2, 2, 1, 'co-production');"

        // Populate movie_info
        "INSERT INTO movie_info (id, movie_id, info_type_id, note) VALUES "
        "(1, 1, 1, 'ranked'), "
        "(2, 2, 2, 'low rank');";

    exec_sql(db, advanced_sql);
}

int main(void)
{
    sqlite3 *db;
    int rc;

    srand(time(NULL)); // Seed the random number generator

    printf("[INFO] Opening database '%s'...\n", DB_NAME);
    rc = sqlite3_open(DB_NAME, &db);
    if (rc != SQLITE_OK)
    {
        fprintf(stderr, "Cannot open database: %s\n", sqlite3_errmsg(db));
        return 1;
    }

    // Create tables
    const char *create_tables_sql =
        "DROP TABLE IF EXISTS TableA;"
        "DROP TABLE IF EXISTS TableB;"
        "CREATE TABLE TableA (key INTEGER, valA TEXT);"
        "CREATE TABLE TableB (key INTEGER, valB TEXT);";

    printf("[INFO] Creating tables...\n");
    exec_sql(db, create_tables_sql);

    // Populate tables with random data
    populate_table_a(db, TABLE_A_SIZE);
    populate_table_b(db, TABLE_B_SIZE);

    // Populate advanced tables
    populate_advanced_tables(db);

    printf("[INFO] Database population completed.\n");

    sqlite3_close(db);
    return 0;
}

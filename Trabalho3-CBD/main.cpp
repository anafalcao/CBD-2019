// #include "util.h"
// #include "table.h"
#include "tablebenchmark.h"
#include "joinbenchmark.h"
#include <stdio.h>

using namespace std;

int main() {    
    
    Table person_table("person");
    person_table.importSchema("person_schema.txt");
    
    // for (long i = 0; i < 2714; i++)
        person_table.convertFromCSV("person.csv");
    person_table.print(5);
    person_table.printHeaderFile(5);
    // // Cursor cursor = person_table.query("SELECT *");
    
    // TableBenchmark benchmark(&person_table);
    // benchmark.runBenchmark();
    
    Table company_table("company");
    company_table.importSchema("company_schema.txt");
    company_table.convertFromCSV("company.csv");
    
    company_table.print(5);
    company_table.printHeaderFile(5);
    
    Table worked_table("worked");
    worked_table.importSchema("worked_schema.txt");
    worked_table.convertFromCSV("worked.csv");
    
    worked_table.print(5);
    worked_table.printHeaderFile(5);
    
    JoinBenchmark joinbenchmark(&person_table, "_id", &worked_table, "person_id");
    joinbenchmark.runBenchmark();
    
    //cout << "\nNested index join" << endl;
    //Join nested_index_join_result = person_table.join("_id", &worked_table, "person_id", JoinType::NESTED_INDEX);
    //nested_index_join_result.print(20);
    
    // cout << "\nMerge join" << endl;
    // Join merge_join_result = person_table.join("_id", &worked_table, "person_id", JoinType::MERGE);
    // merge_join_result.print(20);
    
    // cout << "\nHash join" << endl;
    // Join hash_join_result = person_table.join("_id", &worked_table, "person_id", JoinType::HASH);
    // hash_join_result.print(20);

    // person_table.drop();
    // company_table.drop();
    // worked_table.drop();
    return 0;
}
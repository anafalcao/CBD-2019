#ifndef JOIN_H
#define JOIN_H

#include "util.h"
#include "schema.h"
#include "cursor.h"
#include "queryable.h"


enum JoinType { NESTED_LOOP, NESTED, MERGE, HASH };

class Join {
private:
    
    vector<Queryable*> tables; 
    vector<vector<long long>> * join_result; 
    void nestedLoopJoin(Queryable *this_table, int this_column_position, Queryable* other_table, int other_column_position);

     void mergeJoin(Queryable *this_table, int this_column_position, Queryable* other_table, int other_column_position);
     
    
     void hashJoin(Queryable *this_table, int this_column_position, Queryable* other_table, int other_column_position);
    
public:

    /**
     * @constructor
     */
    Join(Queryable *this_table, string this_column_name, Queryable* other_table, string other_column_name,  JoinType join_type);
    
    /**
     * @destructor
     */
    ~Join();
    
    void print(int number_of_values = -1);
};

void Join::nestedLoopJoin(Queryable *this_table, int this_column_position, Queryable* other_table, int other_column_position) {
    int counter = 0;

    while (counter != this_table->getHeader()->size()) { 
        
        vector<string> this_row = this_table->getRow(this_table->getHeader()->at(counter).second);

        
        for(int i=0; i < other_table->getHeader()->size(); i++){
            vector<string> other_row = other_table->getRow(other_table->getHeader()->at(i).second);
        
            if(this_row.at(this_column_position) == other_row.at(other_column_position)){
                
                vector<long long> join_row;


                join_row.push_back(this_table->getHeader()->at(counter).second);
                join_row.push_back(other_table->getHeader()->at(i).second);
                this->join_result->push_back(join_row);

                
            }
        }
        counter ++;
    }
}

void Join::hashJoin(Queryable *build_table, int build_table_column_position, Queryable* probe_table, int probe_table_column_position) {
    map<string, long long> hash_table;

    header_t* hash_header = build_table->getHeader();
    
    for (header_t::iterator it = hash_header->begin(); it != hash_header->end(); it++) {
        string column_value = build_table->getValue(it->first, build_table_column_position);
        long long registry_position = it->second;
        
        hash_table.insert(pair<string, long long>(column_value, registry_position));
    }
    
    header_t* probe_header = probe_table->getHeader();
    
    for (header_t::iterator it = probe_header->begin(); it != probe_header->end(); it++) {
        string column_value = probe_table->getValue(it->first, probe_table_column_position);
        
        map<string, long long>:: iterator hash_it = hash_table.find(column_value);
        if (hash_it != hash_table.end()) {
            // Found it
            // cout << "Found " << column_value << endl;
            this->join_result->push_back({hash_it->second, it->second});
        }
    }
}

void Join::mergeJoin(Queryable *this_table, int this_column_position, Queryable* other_table, int other_column_position) {
    // cout << "Start merge join" << endl;
    vector<pair<string, long long>> *table_a = this_table->getColumn(this_column_position);
    vector<pair<string, long long>> *table_b = other_table->getColumn(other_column_position);
    // cout << "Start sort on merge join" << endl;
    sort(table_a->begin(), table_a->end(),
        [](const pair<string, long long> &left, const pair<string, long long> &right){
            return left.first.compare(right.first) <= 0;
        });
    sort(table_b->begin(), table_b->end(),
        [](const pair<string, long long> &left, const pair<string, long long> &right){
            return left.first.compare(right.first) <= 0;
        });
    // cout << "End sort on merge join" << endl;

    int n = table_a->size();
    int m = table_b->size();
    int i = 0;
    int j = 0;
    int l, k;

    // cout << "Start loop on merge join" << endl;
    while(i < n and j < m){ 
        if(table_a->at(i).first.compare(table_b->at(j).first) > 0) {
            j++;
        } else if(table_a->at(i).first.compare(table_b->at(j).first) < 0) {
            i++;
        } else {
            l = i;

            while(l < n and table_a->at(l).first.compare(table_a->at(i).first) == 0) {
                k = j;
                while(k < m and table_b->at(k).first.compare(table_b->at(j).first) == 0) {
                    this->join_result->push_back({table_a->at(l).second, table_b->at(k).second});
                    k++;
                }   
                l++;
            }   

            i = l;
            j = k;
        }   
    }   

    delete table_a;
    delete table_b;

    // cout << this->join_result->size() << endl;
    // cout << "End merge join" << endl;
}

Join::Join(Queryable *this_table, string this_column_name, Queryable* other_table, string other_column_name, JoinType join_type) {
    this->join_result = new vector<vector<long long>>;

    tables.push_back(this_table);
    tables.push_back(other_table);


    int this_column_position = this_table->getSchema().getColPosition(this_column_name);
    int other_column_position = other_table->getSchema().getColPosition(other_column_name);
    
    switch(join_type) {
        case NESTED_LOOP  : nestedLoopJoin(this_table, this_column_position, other_table, other_column_position); break;
        case NESTED  : break; 
        case HASH  : hashJoin(this_table, this_column_position, other_table, other_column_position); break;
        case MERGE  : mergeJoin(this_table, this_column_position, other_table, other_column_position); break;
    }
}

void Join::print(int number_of_values) {
    
    for(int line=0; line < join_result->size(); line++){ 
        if(line==number_of_values) break;

        for(int table_order=0; table_order<tables.size(); table_order++) { 
            long long registry_position = join_result->at(line).at(table_order);
            Queryable* table = tables.at(table_order);
            vector<string> row_partial = table->getRow(registry_position);

            for(int column=0; column < table->getSchema().getCols()->size(); column++) { 
                cout<<row_partial.at(column) << " | ";
            }
     
        }
    cout << endl;
    }
}

Join::~Join() {
    delete this->join_result;
}

#endif //JOIN_H
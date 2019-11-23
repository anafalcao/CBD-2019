#ifndef TABLE_H
#define TABLE_H

#include "util.h"
#include "schema.h"
#include "cursor.h"
#include "queryable.h"
#include "join.h"
#include <fstream>
#include <time.h>
#include <string.h>
#include <algorithm>
#include <utility> //std::pair
#include <stdio.h>


class Table : public Queryable{
private:
    unsigned HEADER_SIZE;

    Schema schema;
    string name;
    string path;
    string header_file_path;
    header_t * header;
    
    friend class TableBenchmark;
    
    void convertAndSave(ofstream *file, string * value, SchemaCol * schema_col);
    
    bool insertOnHeaderFile(HeaderFile * header_file);
    
    void loadHeader();
    
public:
    Table(string name);

    ~Table();
    
    
    void importSchema(const string & path);

    void setSchema(Schema schema);

    Schema getSchema();
    header_t * getHeader();

    long long insert(vector<string> row);
    

    vector<string> getRow(long long registry_position);
    
    vector<string> getRowById(long long _id);
    
    
    Join join(string this_column, Table* other_table, string other_column, JoinType join_type);
    
    void drop();
     
    Cursor query(string q);
     

    Cursor query(
            vector<string> & select,
            vector<string> & where_args,
            vector<string> & where_comparators,
            vector<string> & where_values);
     
    
    void convertFromCSV(const string & path);
    
   
    void print(int number_of_values = -1);
    

    void printHeaderFile(int number_of_values = -1);
     

    vector<pair<string, long long>> *getColumn(string column_name);
    vector<pair<string, long long>> *getColumn(int column_position);
    
   
    string getValue(long long _id, int column_position);    
    

    int getNumberOfRows();
};


Table::Table(string name) {
    this->name = name;
    this->path = name + ".dat";
    this->header_file_path = name + "_h.dat";
    this->header = new header_t();
    loadHeader();
    
    RegistryHeader reg_header;
    Table::HEADER_SIZE = sizeof(reg_header.table_name) + sizeof(reg_header.registry_size) + sizeof(reg_header.time_stamp);
    // cout << "HEADER_SIZE = " << HEADER_SIZE << endl;
    
}

Table::~Table() {
    delete this->header;
}

void Table::importSchema(const string & path) {
    schema.import(path);
}

void Table::setSchema(Schema schema) {
    this->schema = schema;
}

Schema Table::getSchema(){
    return this->schema;
}


header_t * Table::getHeader(){
    return this->header;
}

void Table::loadHeader() {
    ifstream file;
    file.open(header_file_path.c_str(), ios::binary);
    
    while (!file.eof()) {
        HeaderFile header;
        if (!file.read(reinterpret_cast<char *> (&header._id), sizeof(header._id)) ||
            !file.read(reinterpret_cast<char *> (&header.registry_position), sizeof(header.registry_position))) {
                break;
        } else {
            this->header->push_back(pair<decltype(header._id), decltype(header.registry_position) > (header._id, header.registry_position));
        }
    }
    
    file.close();
}

void Table::convertAndSave(ofstream *file, string * string_value, SchemaCol *schema_col) {
    if (schema_col->type == INT32) {
        int value = atoi((*string_value).c_str());
        file->write(reinterpret_cast<char *> (&value), schema_col->getSize());
        // cout << "INT32 " << value << "(" << *string_value << ")" << " | ";
    } else if (schema_col->type == CHAR) {
        char value[schema_col->getSize()];
        strncpy(value, &string_value->c_str()[0], schema_col->getSize());
        file->write(reinterpret_cast<char *> (&value), schema_col->getSize());
        // cout << "CHAR " << value << "(" << *string_value << ")" << " | ";
    } else if (schema_col->type == FLOAT) {
        float value = atof((*string_value).c_str());
        file->write(reinterpret_cast<char *> (&value), schema_col->getSize());
        // cout << "FLOAT " << value << "(" << *string_value << ")" << " | ";
    } else if (schema_col->type == DOUBLE) {
        double value = atof((*string_value).c_str());
        file->write(reinterpret_cast<char *> (&value), schema_col->getSize());
        // cout << "DOUBLE " << value << "(" << *string_value << ")" << " | ";
    } else if (schema_col->type == INT64 || schema_col->type == FOREIGN_KEY) {
        long long value = std::stoll((*string_value).c_str());
        file->write(reinterpret_cast<char *> (&value), schema_col->getSize());
        // cout << "INT64 " << value << "(" << *string_value << ")" << " | ";
    }
}

long long Table::insert(vector<string> row) {
    ofstream file;
    file.open(path.c_str(), ios::binary | ios::app);

    HeaderFile header_file;
    header_file.path = this->header_file_path;
    header_file._id = this->header->size();
    header_file.registry_position = file.tellp(); 
    insertOnHeaderFile(&header_file);
    
   
    RegistryHeader header;
    strncpy(header.table_name, &name.c_str()[0], sizeof(header.table_name));
    header.registry_size = HEADER_SIZE + schema.getSize();
    time (& header.time_stamp);
    
    file.write(header.table_name, sizeof(header.table_name));
    file.write(reinterpret_cast<char *> (& header.registry_size), sizeof(header.registry_size));
    file.write(reinterpret_cast<char *> (& header.time_stamp), sizeof(header.time_stamp));
    
    // cout << "  | " << header.table_name << " " << header.registry_size << " " << header.time_stamp << " | ";
    
    
    string _id_str;
    
    std::stringstream strstream;
    strstream << header_file._id;
    strstream >> _id_str;
    
    
    row.insert(row.begin(), _id_str);
    
    
    vector<SchemaCol>* schema_cols = schema.getCols();
    
    int schema_col_position = 0;
    
    for (vector<string>::iterator row_it = row.begin(); row_it != row.end(); row_it++) {
    
        convertAndSave(&file, &(*row_it), &schema_cols->at(schema_col_position));
        schema_col_position ++;
    }
    // cout << endl;
    
    file.close();
    
    return header_file._id;
}

bool Table::insertOnHeaderFile(HeaderFile * header_file) {
    ofstream file;
    file.open(header_file->path.c_str(), ios::binary | ios::app);
    file.write(reinterpret_cast<char *> (& header_file->_id), sizeof(header_file->_id));
    file.write(reinterpret_cast<char *> (& header_file->registry_position), sizeof(header_file->registry_position));
    
    header->push_back(
        pair<decltype(header_file->_id), decltype(header_file->registry_position)> (
            header_file->_id,
            header_file->registry_position));
    
    file.close();
}

void Table::printHeaderFile(int number_of_values) {
    cout << "Printing " << name << " header file" << endl;
    ifstream file;
    file.open(header_file_path.c_str(), ios::binary);
    int counter = 0;
    
    while (!file.eof() && counter != number_of_values) {
        HeaderFile header;
        if (!file.read(reinterpret_cast<char *> (&header._id), sizeof(header._id)) ||
            !file.read(reinterpret_cast<char *> (&header.registry_position), sizeof(header.registry_position))) {
                break;
        } else {
            cout << header._id << " " << header.registry_position << endl;
        }
        counter ++;
    }
    cout << endl;
    
    file.close();
}

void Table::print(int number_of_values) {
    cout << "Printing " << name << " table" << endl;
    int counter = 0;
    while (counter != number_of_values) {
        // cout << "headerSize= "<< header->size()<< endl;
        if (counter == header->size()) {
            break;
        }
        vector<string> row = getRow(header->at(counter).second);
    
        for (vector<string>::iterator it = row.begin(); it != row.end(); it++) {
            cout << (*it) << " | ";
        }
        
        counter ++;
        cout << endl;
    }
    cout << endl;
}

Cursor Table::query(string q) {

    std::transform(q.begin(), q.end(), q.begin(), ::tolower);

    vector<string> select;
    
    vector<string> where_args;
    vector<string> where_comparators;
    vector<string> where_vals;
    
    bool parsing_select = false;
    bool parsing_where = false;
    
    bool parsing_word = false;
    
    bool parsing_where_arg = false;
    bool parsing_where_comparator = false;
    bool parsing_where_val = false;
    
    string string_buffer;
    
    bool ignore_space = false;
    
    for (string::iterator it = q.begin(); it != q.end(); it++) {
        char character = (*it);
        
        if (character == '\'') {
            ignore_space = !ignore_space;
            continue;
            
        } else if (character != ' ' || character != ',') {
            parsing_word = true;
            
        } else {
            if (character == ' ' && ignore_space) {
                parsing_word = true;
            } else {
                parsing_word = false;
            }
        }
        
        if (!parsing_select && !parsing_where) {
            if (character != ' ') {
                string_buffer += character;
            }
            // cout << string_buffer << endl;
            if (string_buffer == "select") {
                parsing_select = true;
                string_buffer.clear();
            } else if (string_buffer == "where") {
                cout << "Changing to WHERE" << endl;
                parsing_where = true;
                parsing_where_arg = true;
                parsing_select = false;                
                string_buffer.clear();
            }
        } else if (character != ' ' || ignore_space) {
            if (parsing_select) {
                if (character == ',') {
                    
                    where_args.push_back(string_buffer);
                    cout << string_buffer << endl;
                    string_buffer.clear();
                } else {
                    if (parsing_word) {
                        string_buffer += character;
                    } else {
                        parsing_select = false;
                        where_args.push_back(string_buffer);
                        cout << string_buffer << endl;
                        string_buffer.clear();
                    }
                }
                
            } else if (parsing_where) {
                if (character == ',') {
                    if (parsing_where_val) {
                        where_vals.push_back(string_buffer);
                        cout << "Where value = " << string_buffer << endl;
                        string_buffer.clear();
                    }
                    parsing_where_arg = true;
                    parsing_where_comparator = false;
                    parsing_where_val = false;
                } else if (parsing_word) {
                    if (parsing_where_arg) {
                        if (character == '=' || character == '<' || character == '>' || character == '!') {
                            
                            parsing_where_arg = false;
                            parsing_where_comparator = true;
                            where_args.push_back(string_buffer);
                            cout << "Where arg = " << string_buffer << endl;
                            string_buffer.clear();
                        }
                    } else if (parsing_where_comparator) {
                        if (character != '=' || character != '<' || character != '>' || character != '!') {
                      
                            parsing_where_comparator = false;
                            parsing_where_val = true;
                            where_comparators.push_back(string_buffer);
                            cout << "Where comparator = " << string_buffer << endl;
                            string_buffer.clear();
                        }
                    }
                    
                    string_buffer += character;
                }
            }
        } else if (character == ' ') {
            if (!string_buffer.empty()) {
                if (parsing_select) {
                    select.push_back(string_buffer);
                    cout << "F Select = " << string_buffer << endl;
                    string_buffer.clear();
                    parsing_select = false;
                }
            }
        }
    }
    if (parsing_select) {
        select.push_back(string_buffer);
        cout << "Final select = " << string_buffer << endl;
        
    } else if (parsing_where && parsing_where_val) {
        where_vals.push_back(string_buffer);
        cout << "Final where value = " << string_buffer << endl;
    }
    
    return query(select, where_args, where_comparators, where_vals);
}

Cursor Table::query(vector<string> & select, vector<string> & where_args, vector<string> & where_comparators, vector<string> & where_values) {
    vector<vector <string> > result;
    
    
    Cursor cursor(schema, result);
    return cursor;
}

void Table::convertFromCSV(const string & path) {
    string line;
    
    ifstream file;
    file.open(path.c_str());
    
    if (file.is_open()) {
        
        getline(file, line);
        
    
        while (getline(file, line)) {
            vector<string> words = split(line, ',');
            
            insert(words);
        }
        file.close();
    } else {
        cout << "Unable to open file - " << path << endl;
    }
}

vector<string> Table::getRow(long long registry_position) {
    ifstream file;
    file.open(path.c_str(), ios::binary);
    
    file.seekg(registry_position);
    
    vector<SchemaCol>* schema_cols = schema.getCols();
    vector<string> row;
    
    RegistryHeader header;
    file.read(header.table_name, sizeof(header.table_name));
    file.read(reinterpret_cast<char *> (& header.registry_size), sizeof(header.registry_size));
    file.read(reinterpret_cast<char *> (& header.time_stamp), sizeof(header.time_stamp));
    
    for (vector<SchemaCol>::iterator it = schema_cols->begin(); it != schema_cols->end(); it++) {
        SchemaCol & schema_col = *it;
        ostringstream stream;
        
        if (schema_col.type == INT32) {
            int value;
            file.read(reinterpret_cast<char *> (&value), schema_col.getSize());
            // cout << "INT32 " << value << " | ";
            stream << value;
        } else if (schema_col.type == CHAR) {
            char value[schema_col.getSize()];
            file.read(reinterpret_cast<char *> (&value), schema_col.getSize());
            // cout << "CHAR " << value << " | ";
            stream << value;
        } else if (schema_col.type == FLOAT) {
            float value;
            file.read(reinterpret_cast<char *> (&value), schema_col.getSize());
            // cout << "FLOAT " << value << " | ";
            stream << value;
        } else if (schema_col.type == DOUBLE) {
            double value;
            file.read(reinterpret_cast<char *> (&value), schema_col.getSize());
            // cout << "DOUBLE " << value << " | ";
            stream << value;
        }  else if (schema_col.type == INT64 || schema_col.type == FOREIGN_KEY) {
            long long value;
            file.read(reinterpret_cast<char *> (&value), schema_col.getSize());
            // cout << "INT64 " << value << " | ";
            stream << value;
        }
        
        string string_value;
        string_value = stream.str();
        
        row.push_back(string_value);
    }
    // cout << endl;
    file.close();
    
    return row;
}

vector<string> Table::getRowById(long long _id) {
    vector<string> row;
    
    int idx = distance(header->begin(), lower_bound(header->begin(), header->end(), 
       make_pair(_id, numeric_limits<long long>::min())));
    
   
    auto pair = header->at(idx);
    if (pair.first == _id) {
        row = getRow(pair.second);
        // print(&row);
    }
    return row;
}

void Table::drop() {
    remove(this->path.c_str());
    remove(this->header_file_path.c_str());
    this->header->clear();
}

Join Table::join(string this_column_name, Table* other_table, string other_column_name, JoinType join_type) {
    return Join(this, this_column_name, other_table, other_column_name, join_type);
}

vector<pair<string, long long>> *Table::getColumn(string column_name) {
    int column_position = schema.getColPosition(column_name);
    return getColumn(column_position);
}

vector<pair<string, long long>> *Table::getColumn(int column_position) {
    if (column_position < 0) return NULL;
    
    vector<pair<string, long long>> *table = new vector<pair<string, long long>>;

    string stub;
    for(header_t::iterator i = header->begin(); i != header->end(); i++){
        stub = getRow(i->second).at(column_position);
        table->push_back(make_pair(stub, i->second));
    }

    return table;
}

int Table::getNumberOfRows() {
    return header->size();
}

string Table::getValue(long long _id, int column_position) {
    string value = "";
    vector<string> row = getRowById(_id);
    if (column_position < row.size()) {
        value = row.at(column_position);
    }
    return value;
}
#endif //TABLE_H
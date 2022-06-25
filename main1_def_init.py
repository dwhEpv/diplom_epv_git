#ФУНКЦИИ РАБОТЫ С ТАБЛИЦАМИ

import jaydebeapi

#---------------------------------------------------------------------------------------------------
# ФУНКЦИЯ инициализации таблиц terminals (STG И DWH_DIM_HIST)
def init_tabs_terminal():
    conn = jaydebeapi.connect(
        'oracle.jdbc.driver.OracleDriver',
        'jdbc:oracle:thin:de2hk/bilbobaggins@de-oracle.chronosavant.ru:1521/deoracle',
        ['de2hk', 'bilbobaggins'],
        'ojdbc7.jar'
    )
    cursor = conn.cursor()
    # Инициализация  STG таблицы terminals
    try:
        cursor.execute('''
            CREATE TABLE S_11_STG_terminals(
                terminal_id varchar(128),
                terminal_type varchar(128),
                terminal_city varchar(128),
                terminal_address varchar(128),
                create_dt date                      
             )
        ''')
    except jaydebeapi.DatabaseError:
        print('Таблица уже создана!')
    # Инициализация  DWH_DIM_HIST таблицы terminals
    try:
        cursor.execute('''
            CREATE TABLE S_11_DWH_DIM_terminals_HIST(
                terminal_id varchar(128), --primary key,
                terminal_type varchar(128),
                terminal_city varchar(128),
                terminal_address varchar(128),
                effective_from date default to_date('1900-01-01 00:00:00','YYYY-MM-DD hh24:mi:ss'),  
                effective_to date default to_date('2999-12-31 23:59:59','YYYY-MM-DD hh24:mi:ss'),
                deleted_flg integer default 0        
             )
        ''')
    except jaydebeapi.DatabaseError:
        print('Таблица уже создана!')
    conn.close()

#---------------------------------------------------------------------------------------------------
# ФУНКЦИЯ инициализации таблиц passport_blacklist (STG И DWH_FACT)
def init_tabs_passport():
    conn = jaydebeapi.connect(
        'oracle.jdbc.driver.OracleDriver',
        'jdbc:oracle:thin:de2hk/bilbobaggins@de-oracle.chronosavant.ru:1521/deoracle',
        ['de2hk', 'bilbobaggins'],
        'ojdbc7.jar'
    )
    cursor = conn.cursor()
    # Инициализация  STG таблицы passport_blacklist
    try:
        cursor.execute(''' 
            CREATE TABLE S_11_STG_pssprt_blcklst(
                passport_num varchar(128),
                entry_dt date                                                    
            )
        ''')
    except jaydebeapi.DatabaseError:
        print('Таблица уже создана!')
    # Инициализация DWH_FACT таблицы passport_blacklist
    try:
        cursor.execute(''' 
            CREATE TABLE S_11_DWH_FACT_pssprt_blcklst(
                passport_num varchar(128),
                entry_dt date,
                create_dt date,
                update_dt date                                      
            )
        ''')
    except jaydebeapi.DatabaseError:
        print('Таблица уже создана!')
    conn.close()


#---------------------------------------------------------------------------------------------------
# Функция инициализации таблиц transactions (STG И DWH_FACT)
def init_tabs_transact():
    conn = jaydebeapi.connect(
        'oracle.jdbc.driver.OracleDriver',
        'jdbc:oracle:thin:de2hk/bilbobaggins@de-oracle.chronosavant.ru:1521/deoracle',
        ['de2hk', 'bilbobaggins'],
        'ojdbc7.jar'
    )
    cursor = conn.cursor()
    # Инициализация  STG таблицы transactions
    try:
        cursor.execute(''' 
            CREATE TABLE S_11_STG_transactions(
                transaction_id varchar(128),
                transaction_date date,
                amount decimal,
                card_num varchar(128),
                oper_type varchar(128),                
                oper_result varchar(128),
                terminal varchar(128)                                     
            )
        ''')
    except jaydebeapi.DatabaseError:
        print('Таблица уже создана!')
    # Инициализация  DWH_FACT таблицы transactions
    # transaction_id;transaction_date;amount;card_num;oper_type;oper_result;terminal
    try:
        cursor.execute(''' 
               CREATE TABLE S_11_DWH_FACT_transactions(
                   trans_id varchar(128),
                   trans_date date,
                   card_num varchar(128),
                   oper_type varchar(128),
                   amt decimal,
                   oper_result varchar(128),
                   terminal varchar(128)                   
                   --foreign key (card_num) references cards (card_num)
                   --foreign key (terminal) references S_11_STG_terminals (terminal_id)              
               )
           ''')
    except jaydebeapi.DatabaseError:
        print('Таблица уже создана!')

    conn.close()

#----------------------------------------------------------------------------
# Функция инициализации REP таблицы rep_fraud
def init_tab_rep_fraud():
    conn = jaydebeapi.connect(
        'oracle.jdbc.driver.OracleDriver',
        'jdbc:oracle:thin:de2hk/bilbobaggins@de-oracle.chronosavant.ru:1521/deoracle',
        ['de2hk', 'bilbobaggins'],
        'ojdbc7.jar'
    )
    cursor = conn.cursor()
    try:
        cursor.execute('''
            CREATE TABLE S_11_REP_FRAUD(
                --trans_id varchar(128),
                event_dt date,
                passport varchar(128),
                fio varchar(128),
                phone varchar(128),
                event_type varchar(128),
                report_dt date,
                CONSTRAINT event_type    
                check (event_type in ('t1', 't2','t3', 't4')) 
            )
        ''')
    except jaydebeapi.DatabaseError:
        print('Таблица уже создана!')
    conn.close()


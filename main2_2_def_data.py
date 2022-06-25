# ФУНКЦИИ РАБОТЫ С ДАННЫМИ

import jaydebeapi
import pandas as pd
#import datetime


# 1/1 Функция загрузки данных terminals.xlsx в STG_terminals (один файл)
#     STG_terminals очищается перед записью
#     Вызов данной функции  в def flow_terminals():
def import_stg_terminal(path, date_create_file):
    conn = jaydebeapi.connect(
        'oracle.jdbc.driver.OracleDriver',
       'jdbc:oracle:thin:de2hk/bilbobaggins@de-oracle.chronosavant.ru:1521/deoracle',
        ['de2hk', 'bilbobaggins'],
        'ojdbc7.jar'
    )
    cursor = conn.cursor()
    #создаем DF из xlsx
    df = pd.read_excel(path)
    #делаем копию DF и добавляем поле из второго аргумента в функции
    dfmod1 = df.assign(create_dt = date_create_file)
    # для java передал с явным преобразованием в str
    df_mod2 = dfmod1.astype({'create_dt': str})
    # Преобразуем DF в list
    df_mod3 = df_mod2.values.tolist()
    print((df_mod3[0][4]), type(df_mod3[0][4]))
    # Очищаем STG перед заполнением
    cursor.execute('TRUNCATE TABLE S_11_STG_terminals')
    cursor.executemany('''
        INSERT INTO S_11_STG_terminals (
            terminal_id,
            terminal_type,
            terminal_city,
            terminal_address,
            create_dt
        )
        VALUES (?, ?, ?, ?, to_date( ?, 'YYYY-MM-DD hh24:mi:ss'))''', df_mod3
    )
    #conn.commit()
    conn.close()

# 1/2 Функция заполнения исторической таблицы terminals (Версионность SCD2)
#     Вызов данной функции  в def flow_terminals():

def add_DWH_HIST_terminal():
    conn = jaydebeapi.connect(
        'oracle.jdbc.driver.OracleDriver',
        'jdbc:oracle:thin:de2hk/bilbobaggins@de-oracle.chronosavant.ru:1521/deoracle',
        ['de2hk', 'bilbobaggins'],
        'ojdbc7.jar'
    )
    cursor = conn.cursor()
    # 1 НОВЫЕ ЗАПИСИ (НЕТ id В S_11_DWH_DIM_TERMINALS_HIST)
    # 1.1.Выборка новых записей
    cursor.execute('''
    SELECT 
        t1.terminal_id,
        t1.terminal_type,
        t1.terminal_city,
        t1.terminal_address,
        t1.create_dt
    FROM  S_11_STG_TERMINALS t1
    LEFT JOIN S_11_DWH_DIM_TERMINALS_HIST t2
    ON t1.terminal_id = t2.terminal_id
    WHERE t2.terminal_id IS NULL
            ''')
    list = cursor.fetchall()
    # 1.2. Добавление новых записей
    cursor.executemany('''
        INSERT INTO S_11_DWH_DIM_terminals_HIST (
        terminal_id,
        terminal_type,
        terminal_city,
        terminal_address,
        effective_from
        )
        VALUES (?, ?, ?, ?, to_date( ?, 'YYYY-MM-DD hh24:mi:ss'))
        ''', list)
    # 2 ДОБАВЛЕНИЕ ВЕРСИОННЫХ СТРОК
    # 2.1. Выборка terminal_id для закрытия даты
    cursor.execute('''
        SELECT 
            t1.terminal_id
        FROM  S_11_STG_TERMINALS t1
        JOIN S_11_DWH_DIM_TERMINALS_HIST t2
            ON   t1.terminal_id = t2.terminal_id
            and (t1.terminal_type    <> t2.terminal_type
            or   t1.terminal_city    <> t2.terminal_city
            or   t1.terminal_address <> t2.terminal_address
            )                 
        ''')
    list2 = cursor.fetchall()
    # 2.2 Закрываем дату у измененных
    cursor.executemany('''
        UPDATE S_11_DWH_DIM_terminals_HIST
        -- Не знаю как передать сюда date_create_file, поэтому select значения даты из любой записи поля в STG terminals.create_dt 
        -- в STG terminals.create_dt всегда записи только из одного файла, поэтому дата одна в каждой строке
        -- проблема в том, что хронологии последовательности формирования списка файлов в директории к обработки может быть не по дате,
        -- соответсвенно может быть закрытие не той датой, будут пересечения периодов.
        SET effective_to = (select create_dt from S_11_STG_terminals where rownum = 1) - 1/24/60/60
        WHERE terminal_id = ? and effective_to = to_date('2999-12-31 23:59:59','YYYY-MM-DD hh24:mi:ss')
    ''', list2)
    # 2.3. Выбираем измененные строки
    cursor.execute('''
           SELECT 
               t1.terminal_id,
               t1.terminal_type,
               t1.terminal_city,
               t1.terminal_address,
               t1.create_dt
           FROM  S_11_STG_TERMINALS t1
           JOIN S_11_DWH_DIM_TERMINALS_HIST t2
               ON   t1.terminal_id = t2.terminal_id
               and (t1.terminal_type    <> t2.terminal_type
               or   t1.terminal_city    <> t2.terminal_city
               or   t1.terminal_address <> t2.terminal_address
               )                 
           ''')
    list3 = cursor.fetchall()
    # 2.4. Вставляем версионные строки
    cursor.executemany('''
            INSERT INTO S_11_DWH_DIM_terminals_HIST (
            terminal_id,
            terminal_type,
            terminal_city,
            terminal_address,
            effective_from
            )
            VALUES (?, ?, ?, ?, to_date( ?, 'YYYY-MM-DD hh24:mi:ss'))
            ''', list3)

#----------------------------------------------------------------------
# 2/1 Функция загрузки данных passport_blacklist в STG_pssprt_blcklst
#     Таблица очищается перед записью (в таблице "всегда один файл")
def import_stg_passport(path):
    conn = jaydebeapi.connect(
        'oracle.jdbc.driver.OracleDriver',
        'jdbc:oracle:thin:de2hk/bilbobaggins@de-oracle.chronosavant.ru:1521/deoracle',
        ['de2hk', 'bilbobaggins'],
        'ojdbc7.jar'
    )
    cursor = conn.cursor()
    df = pd.read_excel(path, sheet_name='blacklist')
    #проблему типа данных поля date  <class 'pandas._libs.tslibs.timestamps.Timestamp'>
    # для java передал с явным преобразованием в str
    df = df.astype({'date': str})
    # делаем копию DF и добавляем поле с именем файла. Преобразуем DF в list
    df_mod2 = df.values.tolist()
    cursor.execute('TRUNCATE TABLE S_11_STG_pssprt_blcklst')
    cursor.executemany('''
        INSERT INTO S_11_STG_pssprt_blcklst(
            entry_dt,
            passport_num                        
            )       
        VALUES (to_date(?, 'YYYY-MM-DD hh24:mi:ss'), ?) 
    ''', df_mod2
    )

# 2/2 Функция заполнения таблицы фактов S_11_DWH_FACT_pssprt_blcklst
def add_DWH_passport():
    conn = jaydebeapi.connect(
        'oracle.jdbc.driver.OracleDriver',
        'jdbc:oracle:thin:de2hk/bilbobaggins@de-oracle.chronosavant.ru:1521/deoracle',
        ['de2hk', 'bilbobaggins'],
        'ojdbc7.jar'
    )
    cursor = conn.cursor()
    #Заполняем курсор выборкой из STG таблицы
    cursor.execute('''
        SELECT
            t1.entry_dt,
            t1.passport_num  
        FROM S_11_STG_pssprt_blcklst t1
        WHERE passport_num NOT IN (SELECT t2.passport_num FROM S_11_DWH_FACT_pssprt_blcklst t2)          
    ''')
    row_passp = cursor.fetchall()
    # Выполняем инсерт из курсора в таблицу DWH_FACT_pssprt_blcklst (итерация, для каждого кортежа в курсоре)
    cursor.executemany('''
        INSERT INTO S_11_DWH_FACT_pssprt_blcklst (
            entry_dt,
            passport_num,
            create_dt
            )  
        VALUES (to_date(?, 'YYYY-MM-DD hh24:mi:ss'), ?, sysdate)   
    ''', row_passp)
    conn.close()


#---------------------------------------------------------------------------------------
# 3/1 Функция загрузки данных из transactions.txt в S_11_STG_transactions
def import_stg_transact(path):
    conn = jaydebeapi.connect(
        'oracle.jdbc.driver.OracleDriver',
        'jdbc:oracle:thin:de2hk/bilbobaggins@de-oracle.chronosavant.ru:1521/deoracle',
        ['de2hk', 'bilbobaggins'],
        'ojdbc7.jar'
    )
    cursor = conn.cursor()
    df = pd.read_csv(path, sep=';')
    # делаем копию DF и добавляем поле с именем файла
    #dfmod1 = df.assign(file_name = path)
    # Преобразуем DF в list
    df_mod2 = df.values.tolist()
    # Очищаем STG перед заполнением
    cursor.execute('TRUNCATE TABLE S_11_STG_TRANSACTIONS')
    # выполняем запрос для всех элементов list
    cursor.executemany('''
        INSERT INTO S_11_STG_TRANSACTIONS (
            transaction_id,
            transaction_date,
            amount,
            card_num,
            oper_type,                
            oper_result,
            terminal           
            )        
        VALUES (?, to_date(?, 'YYYY-MM-DD hh24:mi:ss'), ?, ?, ?, ?, ?)''', df_mod2
    )
    #conn.commit()
    conn.close()

# 3/2 Функция заполнения таблицы фактов S_11_DWH_FACT_transactions
def add_DWH_transactions():
    conn = jaydebeapi.connect(
        'oracle.jdbc.driver.OracleDriver',
        'jdbc:oracle:thin:de2hk/bilbobaggins@de-oracle.chronosavant.ru:1521/deoracle',
        ['de2hk', 'bilbobaggins'],
        'ojdbc7.jar'
    )
    cursor = conn.cursor()
    #Заполняем курсор выборкой из STG таблицы
    cursor.execute('''
        SELECT
            transaction_id,
            transaction_date,
            amount,
            card_num,
            oper_type,                
            oper_result,
            terminal   
        FROM S_11_STG_TRANSACTIONS                 
    ''')
    row_transact = cursor.fetchall()
    # Выполняем инсерт из курсора в таблицу S_11_DWH_FACT_transactions (итерация, для каждого кортежа в курсоре)
    cursor.executemany('''
        INSERT INTO S_11_DWH_FACT_transactions (
            trans_id,
            trans_date,
            amt,
            card_num,
            oper_type,
            oper_result,
            terminal  
            )  
        VALUES (?,to_date(?, 'YYYY-MM-DD hh24:mi:ss'), ?, ?, ?, ?, ?)   
    ''', row_transact)
    conn.close()



# ФУНКЦИИ ПОСТРОЕНИЯ ОТЧЕТОВ
# ПО ТРАНЗАКЦИЯМ С ПРИЗНАКАМИ "МОШЕННИЧЕСКИЕ ОПЕРАЦИИ"

import jaydebeapi

# ОТЧЕТ ПРИЗНАК 1 Операции при просроченном или заблокированном паспорте
def report_flow_fraud_type1():
    conn = jaydebeapi.connect(
        'oracle.jdbc.driver.OracleDriver',
        'jdbc:oracle:thin:de2hk/bilbobaggins@de-oracle.chronosavant.ru:1521/deoracle',
        ['de2hk', 'bilbobaggins'],
        'ojdbc7.jar'
    )
    cursor = conn.cursor()
    print('Ожидайте, идет построение отчета по признакам МО "t1"...')
    # 1. Создаем VIEW ОТЧЕТ FRAUD
    cursor.execute('''
        CREATE VIEW S_11_REP_v_FRAUD_tmp AS
            SELECT       
               trn.TRANS_DATE as event_dt,
               cls.PASSPORT_NUM as passport,
               cls.LAST_NAME || ' ' || cls.FIRST_NAME || ' ' || cls.PATRONYMIC as fio,
               cls.PHONE as phone           
            FROM de2hk.S_11_DWH_FACT_transactions trn
            JOIN BANK.CARDS crd
              ON replace(trn.CARD_NUM,' ') = replace(crd.CARD_NUM,' ')
            JOIN BANK.ACCOUNTS acs
              ON crd.ACCOUNT = acs.ACCOUNT
            JOIN BANK.CLIENTS cls
              ON acs.CLIENT = cls.CLIENT_ID
            LEFT JOIN de2hk.S_11_DWH_FACT_pssprt_blcklst psb
              ON cls.PASSPORT_NUM = psb.PASSPORT_NUM
            JOIN de2hk.S_11_DWH_DIM_terminals_HIST trm
              ON trn.TERMINAL = trm.TERMINAL_ID
             and trn.TRANS_DATE between trm.EFFECTIVE_FROM and trm.EFFECTIVE_TO        
            WHERE cls.PASSPORT_VALID_TO is not null
              and trn.TRANS_DATE > cls.PASSPORT_VALID_TO
               or cls.PASSPORT_NUM in (select psb2.PASSPORT_NUM from de2hk.S_11_DWH_FACT_pssprt_blcklst psb2)
    ''')
    # 2. Выбираем из VIEW записи которых нет в итоговой таблице витрины REP_FRAUD  и инсертим в S_11_REP_FRAUD
    cursor.execute('''
        INSERT INTO S_11_REP_FRAUD(            
            event_dt,
            passport,
            fio,
            phone,
            event_type,
            report_dt
            )
            SELECT
                t1.event_dt as event_dt,
                t1.passport as passport,
                t1.fio as fio,
                t1.phone as phone,    
                't1'  as event_type,
                sysdate as report_dt 
            FROM  S_11_REP_v_FRAUD_tmp t1
            LEFT JOIN S_11_REP_FRAUD t2
                   ON t1.event_dt = t2.event_dt
                  and t1.passport = t2.passport
                  and t1.fio = t2.fio
                  and t1.phone = t2.phone
            WHERE t2.event_dt IS NULL
    ''')
    print('Отчет "t1" УСПЕШНО ЗАПИСАН!')
    # 3. Удаляем созданное VIEW
    cursor.execute('''
        DROP VIEW S_11_REP_v_FRAUD_tmp
    ''')
    conn.close()

#----------------------------------------------------------------------------------
# ОТЧЕТ ПРИЗНАК 2 Операции при недействующем договоре
def report_flow_fraud_type2():
    conn = jaydebeapi.connect(
        'oracle.jdbc.driver.OracleDriver',
        'jdbc:oracle:thin:de2hk/bilbobaggins@de-oracle.chronosavant.ru:1521/deoracle',
        ['de2hk', 'bilbobaggins'],
        'ojdbc7.jar'
    )
    cursor = conn.cursor()
    print('Ожидайте, идет построение отчета по признакам МО "t2"...')
    # 1. Создаем VIEW ОТЧЕТ FRAUD
    cursor.execute('''
        CREATE VIEW S_11_REP_v_FRAUD_tmp AS
            SELECT       
               trn.TRANS_DATE as event_dt,
               cls.PASSPORT_NUM as passport,
               cls.LAST_NAME || ' ' || cls.FIRST_NAME || ' ' || cls.PATRONYMIC as fio,
               cls.PHONE as phone           
            FROM de2hk.S_11_DWH_FACT_transactions trn
            JOIN BANK.CARDS crd
              ON replace(trn.CARD_NUM,' ') = replace(crd.CARD_NUM,' ')
            JOIN BANK.ACCOUNTS acs
              ON crd.ACCOUNT = acs.ACCOUNT
            JOIN BANK.CLIENTS cls
              ON acs.CLIENT = cls.CLIENT_ID
            LEFT JOIN de2hk.S_11_DWH_FACT_pssprt_blcklst psb
              ON cls.PASSPORT_NUM = psb.PASSPORT_NUM
            JOIN de2hk.S_11_DWH_DIM_terminals_HIST trm
              ON trn.TERMINAL = trm.TERMINAL_ID
             and trn.TRANS_DATE between trm.EFFECTIVE_FROM and trm.EFFECTIVE_TO
            WHERE trn.TRANS_DATE > acs.VALID_TO
    ''')
    # 2. Выбираем из VIEW записи которых нет в итоговой таблице витрины REP_FRAUD  и инсертим в S_11_REP_FRAUD
    cursor.execute('''
        INSERT INTO S_11_REP_FRAUD(
            event_dt,
            passport,
            fio,
            phone,
            event_type,
            report_dt
            )
            SELECT
                t1.event_dt as event_dt,
                t1.passport as passport,
                t1.fio as fio,
                t1.phone as phone,    
                't2'  as event_type,
                sysdate as report_dt 
            FROM  S_11_REP_v_FRAUD_tmp t1
            LEFT JOIN S_11_REP_FRAUD t2
                   ON t1.event_dt = t2.event_dt
                  and t1.passport = t2.passport
                  and t1.fio = t2.fio
                  and t1.phone = t2.phone
            WHERE t2.event_dt IS NULL           
    ''')
    print('Отчет "t2" УСПЕШНО ЗАПИСАН!')
    # 3. Удаляем созданное VIEW
    cursor.execute('''
            DROP VIEW S_11_REP_v_FRAUD_tmp
            ''')
    conn.close()


#---------------------------------------------------------------------------------
# ОТЧЕТ ПРИЗНАК 3 Операции в разных городах в течение одного часа
def report_flow_fraud_type3():
    conn = jaydebeapi.connect(
        'oracle.jdbc.driver.OracleDriver',
        'jdbc:oracle:thin:de2hk/bilbobaggins@de-oracle.chronosavant.ru:1521/deoracle',
        ['de2hk', 'bilbobaggins'],
        'ojdbc7.jar'
    )
    cursor = conn.cursor()
    print('Ожидайте, идет построение отчета по признакам МО "t3"...')
    # 1. Создаем VIEW ОТЧЕТ FRAUD
    cursor.execute('''
        CREATE VIEW S_11_REP_v_FRAUD_tmp AS
            SELECT
                t1.event_dt,
                t1.passport,
                t1.fio,
                t1.phone
            FROM
                (SELECT
                    trm.TERMINAL_CITY,
                    trn.TRANS_DATE as event_dt,
                    lag(trn.TRANS_DATE) over(partition by crd.CARD_NUM order by trn.TRANS_DATE) as lag_date,
                    lag(trm.TERMINAL_CITY) over(partition by crd.CARD_NUM order by trn.TRANS_DATE) as lag_city,
                    (trn.TRANS_DATE - lag(trn.TRANS_DATE) over(partition by crd.CARD_NUM order by trn.TRANS_DATE))*24*60 as diff_time,
                    cls.PASSPORT_NUM as passport,
                    cls.LAST_NAME || ' ' || cls.FIRST_NAME || ' ' || cls.PATRONYMIC as fio,
                    cls.PHONE as phone
                FROM de2hk.S_11_DWH_FACT_transactions trn
                    JOIN BANK.CARDS crd
                      ON replace(trn.CARD_NUM,' ') = replace(crd.CARD_NUM,' ')
                    JOIN BANK.ACCOUNTS acs
                      ON crd.ACCOUNT = acs.ACCOUNT
                    JOIN BANK.CLIENTS cls
                      ON acs.CLIENT = cls.CLIENT_ID
                    LEFT JOIN de2hk.S_11_DWH_FACT_pssprt_blcklst psb
                      ON cls.PASSPORT_NUM = psb.PASSPORT_NUM
                    JOIN de2hk.S_11_DWH_DIM_terminals_HIST trm
                      ON trn.TERMINAL = trm.TERMINAL_ID
                     and trn.TRANS_DATE between trm.EFFECTIVE_FROM and trm.EFFECTIVE_TO
                ORDER BY crd.CARD_NUM, trn.TRANS_DATE, trm.TERMINAL_CITY
                ) t1
            WHERE t1.TERMINAL_CITY != t1.lag_city
              and diff_time <= 60
    ''')
    # 2. Выбираем из VIEW записи которых нет в итоговой таблице витрины REP_FRAUD и инсертим в S_11_REP_FRAUD
    cursor.execute('''
        INSERT INTO S_11_REP_FRAUD(            
            event_dt,
            passport,
            fio,
            phone,
            event_type,
            report_dt
            )
            SELECT
                t1.event_dt as event_dt,
                t1.passport as passport,
                t1.fio as fio,
                t1.phone as phone,    
                't3'  as event_type,
                sysdate as report_dt 
            FROM  S_11_REP_v_FRAUD_tmp t1
            LEFT JOIN S_11_REP_FRAUD t2
                   ON t1.event_dt = t2.event_dt
                  and t1.passport = t2.passport
                  and t1.fio = t2.fio
                  and t1.phone = t2.phone
            WHERE t2.event_dt IS NULL
           
    ''')
    print('Отчет "t3" УСПЕШНО ЗАПИСАН!')
    # 3. Удаляем созданное VIEW
    cursor.execute('''
        DROP VIEW S_11_REP_v_FRAUD_tmp
        ''')
    conn.close()

#---------------------------------------------------------------------------------
# ОТЧЕТ ПРИЗНАК 4 Подбор суммы в течении 20 минут
def report_flow_fraud_type4():
    conn = jaydebeapi.connect(
        'oracle.jdbc.driver.OracleDriver',
        'jdbc:oracle:thin:de2hk/bilbobaggins@de-oracle.chronosavant.ru:1521/deoracle',
        ['de2hk', 'bilbobaggins'],
        'ojdbc7.jar'
    )
    cursor = conn.cursor()
    print('Ожидайте, идет построение отчета по признакам МО "t4"...')
    # 1. Создаем VIEW ОТЧЕТ FRAUD
    cursor.execute('''
        CREATE VIEW S_11_REP_v_FRAUD_tmp AS
            SELECT
                t2.event_dt as event_dt,
                t2.passport as passport,
                t2.fio as fio,
                t2.phone as phone
            FROM (
                 SELECT                     
                        t1.diff_time,
                        sum(diff_time) over (partition by t1.CARD_NUM order by t1.event_dt
                            rows between 2 preceding AND CURRENT ROW) as sum_diff_time_3prec,
                        t1.amt,
                        t1.lag_amt,
                        t1.lag2_amt,
                        t1.oper_result,
                        t1.lag_oper_result,
                        t1.lag2_oper_result,
                        t1.event_dt,
                        t1.passport,
                        t1.fio,
                        t1.phone    
                 FROM (
                     SELECT
                           trn.CARD_NUM,
                           trn.TRANS_DATE                                                               as event_dt,
                           round((trn.TRANS_DATE - lag(trn.TRANS_DATE) over
                                (partition by crd.CARD_NUM order by trn.TRANS_DATE)) * 24 * 60)         as diff_time,
                           trn.AMT,
                           sum(trn.AMT) over (partition by crd.CARD_NUM order by trn.TRANS_DATE
                               rows between 2 preceding AND CURRENT ROW)                                as sum_AMT_3prec,
                           lag(trn.AMT) over (partition by crd.CARD_NUM order by trn.TRANS_DATE)        as lag_amt,
                           lag(trn.AMT, 2) over (partition by crd.CARD_NUM order by trn.TRANS_DATE)     as lag2_amt,
                           lag(trn.TRANS_DATE) over (partition by crd.CARD_NUM order by trn.TRANS_DATE) as lag_date,
                           trn.OPER_RESULT                                                              as oper_result,
                           lag(trn.OPER_RESULT) over (partition by crd.CARD_NUM
                               order by trn.TRANS_DATE)                                                 as lag_oper_result,
                           lag(trn.OPER_RESULT, 2) over (partition by crd.CARD_NUM
                               order by trn.TRANS_DATE)                                                 as lag2_oper_result,
                           cls.PASSPORT_NUM                                                             as passport,
                           cls.LAST_NAME || ' ' || cls.FIRST_NAME || ' ' || cls.PATRONYMIC              as fio,
                           cls.PHONE                                                                    as phone
                     FROM de2hk.S_11_DWH_FACT_transactions trn
                          JOIN BANK.CARDS crd
                               ON replace(trn.CARD_NUM, ' ') = replace(crd.CARD_NUM, ' ')
                          JOIN BANK.ACCOUNTS acs
                               ON crd.ACCOUNT = acs.ACCOUNT
                          JOIN BANK.CLIENTS cls
                               ON acs.CLIENT = cls.CLIENT_ID
                          LEFT JOIN de2hk.S_11_DWH_FACT_pssprt_blcklst psb
                               ON cls.PASSPORT_NUM = psb.PASSPORT_NUM
                          JOIN de2hk.S_11_DWH_DIM_terminals_HIST trm
                               ON trn.TERMINAL = trm.TERMINAL_ID
                               and trn.TRANS_DATE between trm.EFFECTIVE_FROM and trm.EFFECTIVE_TO
                     ORDER BY crd.CARD_NUM, trn.TRANS_DATE--, trm.TERMINAL_CITY
                 ) t1
            )t2
            WHERE t2.sum_diff_time_3prec <= 20
              and t2.AMT < t2.lag_amt
              and t2.lag_amt < t2.lag2_amt
              and t2.oper_result = 'SUCCESS'
              and t2.lag_oper_result = 'REJECT'
              and t2.lag2_oper_result = 'REJECT'
    ''')
    # 2. Выбираем из VIEW записи которых нет в итоговой таблице витрины REP_FRAUD и инсертим в S_11_REP_FRAUD
    cursor.execute('''
        INSERT INTO S_11_REP_FRAUD(            
            event_dt,
            passport,
            fio,
            phone,
            event_type,
            report_dt
            )
            SELECT
                t1.event_dt as event_dt,
                t1.passport as passport,
                t1.fio as fio,
                t1.phone as phone,    
                't4'  as event_type,
                sysdate as report_dt 
            FROM  S_11_REP_v_FRAUD_tmp t1
            LEFT JOIN S_11_REP_FRAUD t2
                   ON t1.event_dt = t2.event_dt
                  and t1.passport = t2.passport
                  and t1.fio = t2.fio
                  and t1.phone = t2.phone
            WHERE t2.event_dt IS NULL
    ''')
    print('Отчет "t4" УСПЕШНО ЗАПИСАН!')
    # 3. Удаляем созданное VIEW
    cursor.execute('''
            DROP VIEW S_11_REP_v_FRAUD_tmp
            ''')
    conn.close()
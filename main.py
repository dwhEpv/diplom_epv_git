# ЗАПУСК ПРОЦЕССОВ main.py
# таблицы удалить
#

# Импорт функций созданных для проекта
from main1_def_init import *
from main2_1_def_file import *
from main3_def_REPORT import *


# \1/ ИНИЦИАЛИЗАЦИЯ ТАБЛИЦ (таблиц terminals, passport_blacklist, transactions, rep_fraud)
init_tabs_terminal()
init_tabs_passport()
init_tabs_transact()
init_tab_rep_fraud()

# \2/ ОБРАБОТКА ФАЙЛОВ
### Сделано, протестированно
flow_terminals()
backup_terminals()
flow_passport()
backup_passport()
flow_transact()
backup_transact()

# \3/ ПОСТРОЕНИЕ ОТЧЕТА
report_flow_fraud_type1()
report_flow_fraud_type2()
report_flow_fraud_type3()
report_flow_fraud_type4()








## ЧЕРНОВИК
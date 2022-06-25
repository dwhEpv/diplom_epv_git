#ФУНКЦИИ ОБРАБОТКИ ФАЙЛОВ

# не стал использовал os.system() для передаче операционке сервера команд,
# из-за экономии времени (проект большой, долго отлавливать исключения при работе с сервером)

import os
import re
import shutil
from main2_2_def_data import *

#-------------------------------------------------------------------------------------------------
# ФУНКЦИЯ ОБРАБОТКИ ФАЙЛОВ  "terminals"
# Чтение списка файлов в корне директории проекта и запуск,соответствующих файлу, функций обработки
def flow_terminals():
    #Находим файлы "terminals" и считаем их количество
    list_dir = os.listdir('data_in')
    x = 0
    for i in range(len(list_dir)):
        if 'terminals' in list_dir[i]:
            x += 1
    print(f'Будет обработано {x} файла/ов "terminals"')
    # Формируем список файлов в директории и проходя циклом находим нужный
    for i in range(len(list_dir)):
        if 'terminals' in list_dir[i]:
            print(list_dir[i])
            # Таблица с терминалами точно должна быть HIST, т.к. оборудование постоянно перемещают.
            # Извлекаем дату из имени файла terminals сначала для записи в S_11_STG_terminals.create_dt
            # Потом для дат версионных записей в terminals_HIST.
            # Существование "в параллели" двух дат у разных сущностей,
            # (например 2021.03.01 у фактов транзакций и текущая дата у версионных записей терминалов.)
            # обеспечит выборку нужных строк актуальности терминалов на конкретную дату путем постоянного вычета разницы между ними,
            # Но считаю, что корректная дата начала актуальности действия информации
            # всегда должа, в точности соответсвовать дате ее формирования, без всяких поправок при формировании выборок.
            path_split = re.split('[_.]', list_dir[i])
            date_create_file = path_split[1][4:10] + '-' + path_split[1][2:4] + '-' + path_split[1][0:2]
            # ВЫЗОВ функции загрузки данных в STG таблицу terminals. Вторым аргументом подставляем дату создания файла
            import_stg_terminal('./data_in/'+list_dir[i], date_create_file)
            print('Файл в таблицу S_11_STG_terminals загружен успешно!')
            print('ОЖИДАЙТЕ, идет загрузка S_11_STG_terminals в HIST таблицу...')
            # ВЫЗОВ функции загрузки данных в HIST таблицу terminals
            add_DWH_HIST_terminal()
            print('Файл в таблицу S_11_DWH_DIM_terminals_HIST загружен успешно!')
    print(f'ВСЕ {x} файла/ов "terminals" обработаны УСПЕШНО!')

# ФУНКЦИЯ отправки файлов "terminals" В АРХИВ
def backup_terminals():
    # 1 Находим и Переименовываем файлы "terminals".
    # Формируем список файлов в директории и проходя циклом находим нужный
    list_dir = os.listdir('data_in')
    for i in range(len(list_dir)):
        if 'terminals' in list_dir[i]:
            # Добавляем расширение backup
            new_name = list_dir[i] + '.backup'
            #Переименовываем файл
            os.rename('./data_in/'+list_dir[i], './data_in/'+new_name)
            print(f'Файл {list_dir[i]} переименован на {new_name}')
    # 2 Находим и Перемещаем файлы "terminals.backup" в папку архива
    list_dir = os.listdir('data_in')
    for i in range(len(list_dir)):
        if '.backup' in list_dir[i]:
            #try:
            shutil.move('./data_in/'+list_dir[i], 'archive')
            #except shutil.Error:
            #    path_file = os.path(new_name)
            #    os.remove(path_file)
            #    print(f'предыдущий  {list_dir[i]} перемещен в папку arhive')
            print(f'Файл {list_dir[i]} перемещен в папку arhive')

#-------------------------------------------------------------------------------------------------
# Функция обработки входящих файлов "passport_blacklist"
# Чтение списка файлов в корне директории проекта и запуск,соответствующих файлу, функций обработки
def flow_passport():
    list_dir = os.listdir('./data_in/')
    x = 0
    for i in range(len(list_dir)):
        if 'passport_blacklist' in list_dir[i]:
            x += 1
    print(f'Будет обработано {x} файла/ов "passport_blacklist"')
    for i in range(len(list_dir)):
        if 'passport_blacklist' in list_dir[i]:
            print(list_dir[i])
            # ВЫЗОВ функции1
            import_stg_passport('./data_in/'+list_dir[i])
            print('Файл в таблицу S_11_STG_pssprt_blcklst загружен успешно!')
            # ВЫЗОВ функции2
            print('ОЖИДАЙТЕ, идет загрузка S_11_STG_pssprt_blcklst в FACT таблицу...')
            add_DWH_passport()
            print('Файл в таблицу S_11_DWH_FACT_pssprt_blcklst загружен успешно!')
    print(f'ВСЕ {x} файла/ов "terminals" обработаны УСПЕШНО!')

# ФУНКЦИЯ отправки файлов "passport_blacklist" В АРХИВ
def backup_passport():
    # 1 Находим и Переименовываем файлы "passport_blacklist".
    # Формируем список файлов в директории и проходя циклом находим нужный
    list_dir = os.listdir('./data_in/')
    for i in range(len(list_dir)):
        if 'passport' in list_dir[i]:
            # Добавляем расширение backup
            new_name = list_dir[i] + '.backup'
            # Переименовываем файл
            os.rename('./data_in/'+list_dir[i], './data_in/'+new_name)
            print(f'Файл {list_dir[i]} переименован на {new_name}')
    # 2 Находим и Перемещаем файлы "passport.backup" в папку архива
    list_dir = os.listdir('./data_in/')
    for i in range(len(list_dir)):
        if '.backup' in list_dir[i]:
            shutil.move('./data_in/'+list_dir[i], 'archive')
            print(f'Файл {list_dir[i]} перемещен в папку /arhive')

#-------------------------------------------------------------------------------------------------
# Функция обработки входящих файлов "transactions"
# Чтение списка файлов в корне директории проекта и запуск,соответствующих файлу, функций обработки
def flow_transact():
    list_dir = os.listdir('./data_in/')
    x = 0
    for i in range(len(list_dir)):
        if 'transactions' in list_dir[i]:
            x += 1
    print(f'Будет обработано {x} файла/ов "transactions"')
    for i in range(len(list_dir)):
        if 'transactions' in list_dir[i]:
            print(list_dir[i])
            # ВЫЗОВ функции1
            import_stg_transact('./data_in/'+list_dir[i])
            print('Файл в таблицу S_11_STG_transactions загружен успешно!')
            # ВЫЗОВ функции2
            print('ОЖИДАЙТЕ, идет загрузка S_11_STG_transactions в FACT таблицу...')
            add_DWH_transactions()
            print('Файл в таблицу S_11_DWH_FACT_transactions загружен успешно!')
    print(f'ВСЕ {x} файла/ов "transactions" обработаны УСПЕШНО!')

# ФУНКЦИЯ отправки файлов "transactions" В АРХИВ
def backup_transact():
    # 1 Находим и Переименовываем файлы "transactions".
    # Формируем список файлов в директории и проходя циклом находим нужный
    list_dir = os.listdir('./data_in/')
    for i in range(len(list_dir)):
        if 'transactions' in list_dir[i]:
            # Извлекаем имя без расширения файла transactions
            #path_split = re.split('[.]', list_dir[i])
            # Добавляем расширение backup
            #new_name = path_split[0] + '.backup'
            new_name = list_dir[i] + '.backup'
            # Переименовываем файл
            os.rename('./data_in/'+list_dir[i], './data_in/'+new_name)
            print(f'Файл {list_dir[i]} переименован на {new_name}')
    # 2 Находим и Перемещаем файлы "transactions.backup" в папку архива
    list_dir = os.listdir('./data_in/')
    for i in range(len(list_dir)):
        if '.backup' in list_dir[i]:
            shutil.move('./data_in/'+list_dir[i], 'archive')
            print(f'Файл {list_dir[i]} перемещен в папку /arhive')
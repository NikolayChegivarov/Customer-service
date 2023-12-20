import psycopg2

from config import host, user, password, db_name, port

try:
    # устанавливаем соединение
    connection = psycopg2.connect (
        host=host,
        user=user,
        password=password,
        database=db_name,
        port=port
    )

    connection.autocommit = True  # автоматизирую commit

    # проверка соединения, выводим версию сервера
    with connection.cursor() as cur:
        cur.execute(
            'SELECT version();'
        )
        print(f'Server version {cur.fetchone()}')

    # создаю схему
    with connection.cursor() as cur:
        cur.execute(
            'CREATE SCHEMA IF NOT EXISTS personal_information;'
        )
        print(f'SCHEMA CREATE')

    # подключаюсь к схеме
    with connection.cursor() as cur:
        cur.execute(
            'set search_path to "personal_information";'
        )
        print(f'SCHEMA CONNECT')

    list_of_commands = ("""
        CREATE_TABLE -- Функция, создающая структуру БД (таблицы).
        NEW_CLIENT -- Функция, позволяющая добавить нового клиента.
        NEW_PHONE -- Функция, позволяющая добавить телефон для существующего клиента.
        CHANGE_CLIENT -- Функция, позволяющая изменить данные о клиенте.
        DELITE_PHONE -- Функция, позволяющая удалить телефон для существующего клиента.
        DELITE_CLIENT -- Функция, позволяющая удалить существующего клиента.
        SEARCH_CLIENT -- Функция, позволяющая найти клиента по его данным: имени, фамилии, email или телефону."""
                        )
    print(list_of_commands)
    command = input('Ввидите команду: ')

    # общая функция программы
    def get_command(command):

        # Функция, создающая структуру БД (таблицы).
        if command == 'CREATE_TABLE':
            def CREATE_TABLE():
                with connection.cursor() as cur:
                    cur.execute(
                        """CREATE TABLE IF NOT EXISTS users (
                        id_users SERIAL PRIMARY KEY,
                        name_users VARCHAR(40) NOT NULL,
                        surname VARCHAR(40)NOT NULL,
                        Email VARCHAR(60) NOT NULL)"""
                    )
                    print(f'TABLE users CREATE')
                with connection.cursor() as cur:
                    cur.execute(
                        """CREATE TABLE IF NOT EXISTS phone_users (
                        id_phone SERIAL PRIMARY KEY,
                        PhoneNumber VARCHAR(40) NOT NULL,
                        id_users int NOT NULL,
                        CONSTRAINT id_users_fkey FOREIGN KEY (id_users) REFERENCES personal_information.users(id_users) ON DELETE CASCADE)"""
                    )
                    print(f'TABLE PhoneNumber CREATE')
            CREATE_TABLE()

        # Функция, позволяющая добавить нового клиента.
        elif command == 'NEW_CLIENT':
            def NEW_CLIENT():
                with connection.cursor() as cur:
                    name_ = input('Введите имя: ')
                    surname_ = input('Введите фамилию: ')
                    email_ = input('Ввидите email: ')
                    cur.execute(
                        """INSERT INTO users(name_users, surname, email)
                        VALUES (%s, %s, %s)
                        """,
                        (name_, surname_, email_)
                    )
                print(f'Клиент {name_} {surname_} заведен.')
            NEW_CLIENT()

        # Функция, позволяющая добавить телефон для существующего клиента.
        elif command == 'NEW_PHONE':
            def NEW_PHONE():
                with connection.cursor() as cur:
                    PhoneNumber_ = input('Введите номер телефона: ')
                    id_users_ = input('Введите id клиента: ')
                    cur.execute(
                        """INSERT INTO phone_users (PhoneNumber, id_users)
                        VALUES (%s, %s)
                        """,
                        (PhoneNumber_, id_users_)
                                )
                print(f'Телефон {PhoneNumber_} клиента {id_users_} заведен.')
            NEW_PHONE()

        # Функция, позволяющая изменить данные о клиенте.
        elif command == 'CHANGE_CLIENT':
            def CHANGE_CLIENT():
                id_users_ = input('Укажите id клиента данные которого хотите поменять: ')
                CHANGE_CLIENT = input('Что вы хотите изменить? name_users, surname, email, phoneNumber: ')
                required_value = input(f'На какое значение хотите изменить {CHANGE_CLIENT} клиента?: ')
                with connection.cursor() as cur:
                    # если изменяем телефон
                    if CHANGE_CLIENT == 'phoneNumber':
                        # cursor выводит номера телефонов клиента
                        cur.execute(
                            """select phoneNumber, id_phone
                            from users u
                            join phone_users pn on u.id_users = pn.id_users
                            where u.id_users = (%s)
                            """,
                            (id_users_)
                        )
                        phone_numbers = cur.fetchall()
                        # если нет телефонов
                        if phone_numbers == []:
                            print('У данного клиента нет телефонного номера')
                        # если есть телефон(ы)
                        elif phone_numbers != []:
                            # имя фамилия
                            cur.execute(
                                """select name_users, surname
                                from users u
                                join phone_users pn on u.id_users = pn.id_users
                                where u.id_users = (%s)
                                limit 1
                                """,
                                (id_users_)
                            )
                            name_surname = cur.fetchall()
                            # выбор, изменение номера
                            choice_numbers = input(f'Выберите цифру телефона который хотите изменить {name_surname} : {phone_numbers}')
                            sql_ = f"""
                            UPDATE phone_users
                            SET {CHANGE_CLIENT} = %s
                            WHERE id_phone = %s """
                            cur.execute(sql_, (required_value, choice_numbers))
                            print(f' {CHANGE_CLIENT} изменен на {required_value}.')
                    # если изменяем имя, фамилию, email
                    else:
                        sql_ = f"""
                        UPDATE users
                        SET {CHANGE_CLIENT} = %s
                        WHERE id_users = %s """
                        cur.execute(sql_, (required_value, id_users_))
                        print(f' {CHANGE_CLIENT} клиента {id_users_} изменен на {required_value}.')
            CHANGE_CLIENT()

        # Функция, позволяющая удалить телефон для существующего клиента.
        elif command == 'DELITE_PHONE':
            def DELITE_PHONE():
                with (connection.cursor() as cur):
                    id_users_ = input('Введите id клиента номер телефона которого хотите удалить: ')
                    # cursor выводит номера телефонов клиента
                    cur.execute(
                        """select phoneNumber, id_phone
                        from users u
                        join phone_users pn on u.id_users = pn.id_users
                        where u.id_users = (%s)
                        """,
                        (id_users_)
                    )
                    phone_numbers = cur.fetchall()
                    # если нет телефонов
                    if phone_numbers == []:
                        print('У данного клиента нет телефонного номера')
                    # если есть телефон(ы)
                    elif phone_numbers != []:
                        # имя фамилия
                        cur.execute(
                            """select name_users, surname
                            from users u
                            join phone_users pn on u.id_users = pn.id_users
                            where u.id_users = (%s)
                            limit 1
                            """,
                            (id_users_)
                        )
                        name_surname = cur.fetchall()
                        # выбор, удаление номера
                        choice_numbers = input(f'Выберите цифру телефона который хотите удалить {name_surname} : {phone_numbers}')
                        cur.execute(
                            """DELETE FROM phone_users
                            WHERE id_phone = (%s)
                            """,
                            (choice_numbers,)
                        )
                        print(f'телефон удален')
            DELITE_PHONE()

        # Функция, позволяющая удалить существующего клиента.
        elif command == 'DELITE_CLIENT':
            def DELITE_CLIENT():
                with (connection.cursor() as cur):
                    id_users_ = input('Введите id клиента которого хотите удалить: ')
                    cur.execute(
                        """DELETE FROM users CASCADE
                        WHERE id_users = (%s)
                        """,
                        (id_users_,)
                    )
                    print(f'клиент {id_users_} удален')
                    cur.execute(
                        """DELETE FROM phone_users
                        WHERE id_users = (%s)
                        """,
                        (id_users_,)
                    )
            DELITE_CLIENT()

        # функция осуществляющая поиск клинета
        elif command == 'SEARCH_CLIENT':
            def SEARCH_CLIENT():
                parameters = []
                query = "SELECT name_users, surname, email, phonenumber FROM users u FULL OUTER JOIN phone_users pn ON u.id_users = pn.id_users WHERE "
                column_names = ['name_users', 'surname', 'email', 'phoneNumber']
                for column_name in column_names:
                    value_ = input(f'Укажите {column_name}: ')
                    if value_:  # добавлять к запросу только в том случае, если пользователь ввел значение
                        parameters.append((value_,))
                        query += f"{column_name} = %s AND "
                query = query.rstrip(" AND ")
                with connection.cursor() as cur:
                    cur.execute(query, parameters)
                    result = cur.fetchall()
                    if result == []:
                        print(f'Такого клиента нет в базе данных.')
                    else:
                        print(result)
            SEARCH_CLIENT()

        else:
            print('Такой команды нет')

    get_command(command)
except Exception as _ex:
    print('[INFO] Error while working with PosgreSQL', _ex)

finally:
    if connection:
        connection.close()
        print('[INFO] PosgreSQL connection closed')




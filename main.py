import psycopg2


def create_tables(conn, cur):
    cur.execute("""
            CREATE TABLE IF NOT EXISTS clients_info(
                client_id SERIAL PRIMARY KEY,
                first_name VARCHAR(20),
                last_name VARCHAR(30),
                email VARCHAR(80)
            );
            
            CREATE TABLE IF NOT EXISTS phones(
                id SERIAL PRIMARY KEY,
                phone_number NUMERIC, 
                client_id INTEGER references clients_info(client_id)
            );
            """)
    conn.commit()


def add_client(cur, first_name, last_name, email, phone=None):
    if first_name == None or last_name == None or email == None:
        print('Клиент не добавлен - не заполнены основные поля Имя/Фамилия/E-mail')
        return

    cur.execute("""
        INSERT INTO clients_info(first_name, last_name, email)
        VALUES(%s, %s, %s)
        RETURNING client_id, first_name, last_name;
        """, (first_name, last_name, email))
    new_client = cur.fetchone()
    print(new_client[0])
    if phone is not None:
        cur.execute("""
            INSERT INTO phones(client_id, phone_number)
            VALUES(%s, %s);
            """, (new_client[0], phone))
        conn.commit()
    print(f'Добавлен клиент {new_client}')


def get_phone(cur, client_id, phone):
    cur.execute("""
        SELECT phone_number FROM phones 
        WHERE client_id=%s AND phone_number=%s;
        """, (client_id, phone))
    return cur.fetchall()


def add_phone(cur, client_id, phone):
    found_phone = get_phone(cur, client_id, phone)
    if found_phone is None or len(found_phone) == 0:
        cur.execute("""
            INSERT INTO phones(client_id, phone_number) VALUES(%s, %s);
            """, (client_id, phone))
        conn.commit()
        print(f'Телефон {phone} для клиента {client_id} добавлен')
    else:
        print('Данный номер уже имеется в базе')


def change_client(conn, cur, client_id, first_name=None, last_name=None, email=None, phone=None):
    if first_name is not None:
        cur.execute("""
            UPDATE clients_info SET first_name=%s WHERE client_id=%s
            """, (first_name, client_id))
    if last_name is not None:
        cur.execute("""
            UPDATE clients_info SET last_name=%s WHERE client_id=%s
            """, (last_name, client_id))
    if email is not None:
        cur.execute("""
            UPDATE clients_info SET email=%s WHERE client_id=%s
            """, (email, client_id))
    if phone is not None:
        add_phone(cur, client_id, phone)

    cur.execute("""
        SELECT * FROM clients_info;
        """)


def delete_phone(cur, client_id, phone):
    cur.execute("""
        DELETE FROM phones WHERE client_id=%s and phone_number=%s;
        """, (client_id, phone))
    cur.execute("""
        SELECT * FROM phones WHERE client_id=%s;
        """, (client_id,))
    print(cur.fetchall())


def delete_client(cur, client_id):
    cur.execute("""
        DELETE FROM phones WHERE client_id=%s;
        """, (client_id,))
    cur.execute("""
        DELETE FROM clients_info WHERE client_id=%s;
        """, (client_id,))
    cur.execute("""
        SELECT * FROM clients_info;
        """)
    cur.fetchall()


def find_client(cur, first_name=None, last_name=None, email=None, phone=None):
    if phone is not None:
        cur.execute("""
            SELECT cl.*, ph.phone_number FROM clients_info cl
            JOIN phones ph USING (client_id)
            WHERE ph.phone_number=%s;
            """, (phone,))
    else:
        cur.execute("""
            SELECT * FROM clients_info 
            WHERE first_name=%s OR last_name=%s OR email=%s;
            """, (first_name, last_name, email))
    print(cur.fetchall())


def all_clients(cur):
    cur.execute("""
        SELECT * FROM clients_info;
        """)
    print(cur.fetchall())
    cur.execute("""
        SELECT * FROM phones;
        """)
    print(cur.fetchall())


if __name__ == '__main__':
    with psycopg2.connect(database="ClientsDB", user="postgres", password=123456) as conn:
        with conn.cursor() as cur:
            create_tables(conn, cur)

            add_client(cur, 'Иван', 'Иванов', 'ivanov@ya.ru')
            add_client(cur, 'Петр', 'Петров', 'petrov@ya.ru', 79161111111)
            add_client(cur, 'Максим', 'Максимов', 'maximov@ya.ru', 79162222222)

            all_clients(cur)

            # add_phone(cur, 1, 79163333333)
            # add_phone(cur, 2, 79164444444)
            # add_phone(cur, 3, 79165555555)
            # all_clients(cur)

            # change_client(conn, cur, 1, 'Олег')
            # change_client(conn, cur, 2, last_name='Федоров')
            # change_client(conn, cur, 2, email= 'fedorov@ya.ru')
            # change_client(conn, cur, 3, phone=79160000000)
            # all_clients(cur)

            # delete_phone(cur, 1, 79163333333)
            # all_clients(cur)

            # find_client(cur, first_name='Олег')
            # find_client(cur, last_name='Максимов')
            # find_client(cur, email='fedorov@ya.ru')
            # find_client(cur, phone=79160000000)
            # all_clients(cur)

            # delete_client(cur, 3)
            # all_clients(cur)

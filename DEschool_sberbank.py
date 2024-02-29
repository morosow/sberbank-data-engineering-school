import sqlite3

import pandas as pd


class Market:

    def __init__(self, name='dbo'):
        """
        Initialize a database and all required tables.
        Original data imports from csv file, validates and then inserts into db.

        Parameters
        ----------
            database : Database's name. Can be w/- or w/o '.db' at the end. Default: dbo

            log_db : Database's name, that logs information about all changes in main DB.

            category_df, goods_df, customers_df, locators_df : Dataframes with data, prepared to insert into database

        """

        self.database = self.dbname_check(name)
        self.log_db = 'log_' + self.dbname_check(name)
        self.db_create()

        self.category_df = self.category_validation(
            self.csv_import(file='categoris_table.csv', table='categories'))
        self.category_insert()

        self.goods_df = self.goods_validation(
            self.csv_import(file='goods_table.csv', table='goods'))
        self.goods_insert()

        self.customers_df = self.customers_validation(
            self.csv_import(file='Persons_table.csv', table='customers'))
        self.customers_insert()

        self.locators_df = self.customers_df[[
            'first_name', 'last_name', 'email', 'additionalInfo'
        ]][:]
        self.locators_insert()

    def db_create(self):
        """Create all required Tables via SQL-query and logs it.

            Tables:
            -------
                log_table : Keep all information about changes

                Goods : Available goods on warehouse. Each article has its own id.

                Locators : First name, last name, email and info.

                Customers : First name, last name, sex and additional info.

                Categories : Good's category.

                Deliveries : Keep information about new good's delivery.

                Transactions : All information about money transactions.

                Categories_sales : Sales on categories.

                Customers_sales : Personal discount.

        """

        con = sqlite3.connect(self.log_db)
        cursor = con.cursor()
        cursor.execute('''DROP TABLE IF EXISTS log_table''')
        cursor.execute('''CREATE TABLE IF NOT EXISTS log_table(
                    id integer not null primary key autoincrement,
                    dttm date default current_timestamp,
                    operation nvarchar(255),
                    subject nvarchar(255),
                    data nvarchar(255)
                    )''')
        con.commit()

        self.add_log(operation='create', subject='log_table')

        con = sqlite3.connect(self.database)
        cursor = con.cursor()

        cursor.execute('''DROP TABLE IF EXISTS Goods;''')
        cursor.execute('''DROP TABLE IF EXISTS Locators;''')
        cursor.execute('''DROP TABLE IF EXISTS Customers;''')
        cursor.execute('''DROP TABLE IF EXISTS Categories;''')
        cursor.execute('''DROP TABLE IF EXISTS Deliveries;''')
        cursor.execute('''DROP TABLE IF EXISTS Transactions;''')
        cursor.execute('''DROP TABLE IF EXISTS Categories_sales;''')
        cursor.execute('''DROP TABLE IF EXISTS Customers_sales;''')

        # Creating Goods table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Goods(
            id integer not null primary key autoincrement,
            title nvarchar(255),
            price integer,
            categoryId integer,
            delflg char(1) default 0
            );
        ''')

        self.add_log(operation='create', subject='Goods')

        # Creating Categories table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Categories (
            id integer not null primary key autoincrement,
            title nvarchar(255) not null,
            description nvarchar(255) not null,
            delflg char(1) default 0
            );''')

        self.add_log(operation='create', subject='Categories')

        # Add default category
        cursor.execute('''
                        insert into Categories(id,title,description)
                        values(0,\'Нет категории\', \'Категория отсутствует в базе или не указана\');'''
                       )

        self.add_log(operation='insert', subject='Categories')

        # Creating Locators table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Locators (
            first_name nvarchar(255) not null,
            last_name nvarchar(255) not null,
            email nvarchar(255),
            additionalInfo nvarchar(255) default '',
            delflg char(1) default 0,
            primary key (first_name,last_name)
            );   
        ''')

        self.add_log(operation='create', subject='Locators')

        # Creating Customers table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Customers (
            id integer not null primary key autoincrement,
            first_name nvarchar(255) not null,
            last_name nvarchar(255) not null,
            gender nvarchar(6) default null,
            additionalInfo nvarchar(255) default null,
            delflg char(1) default 0
            );

        ''')

        self.add_log(operation='create', subject='Customers')

        # Creating Deliveries table
        cursor.execute('''
                    CREATE TABLE IF NOT EXISTS Deliveries (
                    id integer not null primary key autoincrement,
                    title nvarchar(255) not null,
                    category_id numeric default 0,
                    quantity numeric default 0,
                    price numeric default 0,
                    additionalInfo nvarchar(255) default null
                    );

                ''')

        self.add_log(operation='create', subject='Deliveries')

        # Creating Transactions table (subject - delivery or good, depends from type)
        cursor.execute('''
                    CREATE TABLE IF NOT EXISTS Transactions (
                    id integer not null primary key autoincrement,
                    type nvarchar(255) not null,
                    subject_id numeric not null,
                    customer_id numeric default null,
                    quantity numeric default 1,
                    total numeric default 0,
                    discount numeric default 0,         
                    additionalInfo nvarchar(255) default null,
                    date date default current_date
                    );
                ''')

        self.add_log(operation='create', subject='Transactions')

        # Creating Sales to categories table
        cursor.execute('''
                    CREATE TABLE IF NOT EXISTS Categories_sales (
                    id integer not null primary key autoincrement,
                    category_id numeric default 0,
                    title nvarchar(255) default null,
                    discount numeric default 0,         
                    active nvarchar(1) default 0
                    );
                ''')

        self.add_log(operation='create', subject='Categories_sales')

        # Creating Sales to customers table
        cursor.execute('''
                    CREATE TABLE IF NOT EXISTS Customers_sales (
                    id integer not null primary key autoincrement,
                    customer_id numeric default 0,
                    title nvarchar(255) default null,
                    discount numeric default 0,         
                    active char(1) default 0
                    );
                ''')

        self.add_log(operation='create', subject='Customers_sales')

        con.commit()

    def csv_import(self, file='categoris_table.csv', table=None):
        """Import original data from csv to database"""

        raw_df = pd.read_csv(file, sep='\n', names=['chunk'])
        imported_df = raw_df
        if table == 'categories':
            imported_df = self.import_converting(
                raw_df, 3, ['id', 'title', 'description'])
        if table == 'goods':
            imported_df = self.import_converting(
                raw_df, 4, ['id', 'title', 'price', 'categoryId'])
        if table == 'customers':
            imported_df = self.import_converting(
                raw_df, 5,
                ['id', 'first_name', 'last_name', 'email', 'gender'])
        return imported_df

    def table_size(self, db_name):
        """Take database's name and print number of rows. """

        con = sqlite3.connect(self.database)
        cursor = con.cursor()
        rows = cursor.execute('''SELECT COUNT(*) FROM %s''' %
                              db_name).fetchone()[0]
        print('Table', db_name, 'contains %s rows' % rows)
        con.commit()

        return 0

    def db_print(self, table_name, limit=30):
        """
        Take table's name and print rows from it.

        Parameters
        ----------
            limit : Number of rows

            table_name : Name of table from database

        """

        con = sqlite3.connect(self.database)
        cursor = con.cursor()
        for i in cursor.execute('''SELECT * FROM %s LIMIT %s''' %
                                (table_name, limit)):
            print(i)
        con.commit()
        return 0

    def goods_cats(self, limit=30):
        """Print join of 'Goods' and 'Categories' tables"""
        con = sqlite3.connect(self.database)
        cursor = con.cursor()
        for i in cursor.execute(
                '''SELECT Goods.title, Goods.price, Categories.title, Categories.description
                                    FROM Goods
                                    LEFT JOIN Categories
                                    ON Goods.categoryId = Categories.id'''):
            print(i)
        con.commit()
        return 0

    def sql_execution(self, sql_query):
        """Execute SQL query in main database"""

        con = sqlite3.connect(self.database)
        cursor = con.cursor()
        for i in cursor.execute(sql_query).fetchall():
            print(i)
        con.commit()

    def category_insert(self):
        """Insert validated data from dataframe into table Categories."""

        con = sqlite3.connect(self.database)
        cursor = con.cursor()
        try:
            for index, rows in self.category_df.iterrows():
                sql = '''insert into Categories(id,title,description) values(?, ?, ?);'''
                data = (rows.id, rows.title, rows.description)
                cursor.execute(sql, data)
                con.commit()
                self.add_log(operation='insert',
                             subject='Categories',
                             data=str(data))
        except Exception as e:
            print('An error occurred. Database wasn\'t updated.')
            print('Error:', e)

    def goods_insert(self):
        """Insert validated data from dataframe into table Goods."""

        con = sqlite3.connect(self.database)
        cursor = con.cursor()
        try:
            for index, rows in self.goods_df.iterrows():
                sql = '''insert into Goods(id,title,price,categoryId) values(?, ?, ?, ?);'''
                data = (rows.id, rows.title, rows.price, rows.categoryId)
                cursor.execute(sql, data)
                con.commit()
                self.add_log(operation='insert',
                             subject='Goods',
                             data=str(data))
        except Exception as e:
            print('An error occurred. Database wasn\'t updated.')
            print('Error:', e)

    def customers_insert(self):
        """Insert validated data from dataframe into table Customers."""

        con = sqlite3.connect(self.database)
        cursor = con.cursor()
        try:
            for index, rows in self.customers_df[[
                    'id', 'first_name', 'last_name', 'gender'
            ]].iterrows():
                sql = '''insert into Customers(id,first_name,last_name,gender) values(?, ?, ?, ?);'''
                data = (rows.id, rows.first_name, rows.last_name, rows.gender)
                cursor.execute(sql, data)
                con.commit()
                self.add_log(operation='insert',
                             subject='Customers',
                             data=str(data))
        except Exception as e:
            print('An error occurred. Database wasn\'t updated.')
            print('Error:', e)

    def locators_insert(self):
        """Insert validated data from dataframe into table Locators."""

        con = sqlite3.connect(self.database)
        cursor = con.cursor()
        try:
            for index, rows in self.locators_df.iterrows():
                sql = '''SELECT count(*) FROM (SELECT 1 FROM Locators WHERE first_name = ? AND last_name = ?) t1'''
                data = (rows.first_name, rows.last_name)
                if cursor.execute(sql, data).fetchone()[0] == 0:
                    sql = '''insert into Locators(first_name, last_name, email, additionalInfo) values(?,?,?,?)'''
                    data = (rows.first_name, rows.last_name, rows.email,
                            rows.additionalInfo)
                    cursor.execute(sql, data)
                    con.commit()
                    self.add_log(operation='insert',
                                 subject='Locators',
                                 data=str(data))
                else:
                    sql = '''update Locators set additionalInfo = additionalInfo || ? || ? || '; '
                    where first_name=? and last_name=?'''
                    data = (rows.email, rows.additionalInfo, rows.first_name,
                            rows.last_name)
                    cursor.execute(sql, data)
                    con.commit()
                    self.add_log(operation='update',
                                 subject='Locators',
                                 data=str(data))
        except Exception as e:
            print('An error occurred. Database wasn\'t updated.')
            print('Error:', e)
        con.commit()

    def delivery_add(self,
                     title,
                     price,
                     category_id=0,
                     quantity=1,
                     additionalInfo=None):
        """
        Adds delivery as transaction + warehouse replenishment.
        Delivery must contain only one article's type.

        """

        con = sqlite3.connect(self.database)
        cursor = con.cursor()
        sql = '''insert into Deliveries(title, category_id, quantity, price, additionalInfo) values(?,?,?,?,?)'''
        data = (title, category_id, quantity, price, additionalInfo)
        cursor.execute(sql, data)
        con.commit()
        self.add_log(operation='insert', subject='Deliveries', data=str(data))
        self.goods_add(title, price, category_id, quantity)
        self.transactions_add(type='delivery',
                              subject_id=-1,
                              quantity=quantity,
                              total=quantity * price,
                              additionalInfo=additionalInfo)
        print('Record successfully added.')

    def goods_add(self, title, price, categoryId=0, count=0, delflg=0):
        """Add new goods into database. Each row contain only 1 unit of goods"""

        con = sqlite3.connect(self.database)
        cursor = con.cursor()
        sql = '''insert into Goods(title, price, categoryId, delflg) values(?,?,?,?)'''
        for i in range(0, count):
            data = (title, price, categoryId, delflg)
            cursor.execute(sql, data)
            self.add_log(operation='insert', subject='Goods', data=str(data))

        con.commit()

    def transactions_add(self,
                         type,
                         subject_id,
                         total=0,
                         quantity=1,
                         customer_id=None,
                         discount=0,
                         additionalInfo=None,
                         date=pd.datetime.now().date()):
        """
        Add new transaction into database. Transaction's type may be different.
        If type = return or sell, subject_id is good_id, in case of delivery - supplier_id
        delivery gets '-1' as subject_id by default, but it must be another one table 'Suppliers'.

        """
        if not (type == 'delivery' or type == 'return' or type == 'sell'):
            print('Error: incorrect type. Must be delivery, return or sell.')

        elif type == 'sell':
            quantity = 1
            con = sqlite3.connect(self.database)
            cursor = con.cursor()

            if (cursor.execute(
                    '''select count(*)
                                    from Goods
                                    where Goods.id = ? and Goods.delflg = 0''',
                [subject_id]).fetchone()[0]) == 0:
                print('There are no goods at the warehouse')
            else:

                try:  # Getting personal customers discount
                    sql = '''select Customers_sales.discount from customers left join Customers_sales 
                            on customers.id = Customers_sales.customer_id where customers.id = ?'''
                    discount = cursor.execute(
                        sql, [customer_id]).fetchone()[0] / 100
                except:
                    discount = 0

                if discount == 0:
                    try:  # Getting category discount
                        sql = '''select * from Goods 
                                    left join Categories_sales on
                                    Categories_sales.category_id = Goods.categoryId where Goods.id = ?'''
                        if cursor.execute(sql,
                                          [subject_id]).fetchone()[8] is None:
                            discount = 0
                        else:
                            discount = cursor.execute(
                                sql, [subject_id]).fetchone()[8] / 100
                    except Exception as e:
                        print('Error: ', e)
                        discount = 0

                sql = '''select price from Goods where id = ?'''
                price = cursor.execute(sql, [subject_id]).fetchone()[0]
                sql = '''insert into Transactions(type, total, subject_id, quantity, customer_id, discount, 
                                                    additionalInfo, date) 
                        values(?,?,?,?,?,?,?,?)'''
                data = (type, round(price * (1 - discount), 2), subject_id,
                        quantity, customer_id, discount, additionalInfo, date)
                cursor.execute(sql, data)
                self.add_log(operation='insert',
                             subject='Transactions',
                             data=str(data))

                sql = '''update Goods set delflg = 1 where id = ?'''

                cursor.execute(sql, [subject_id])
                self.add_log(operation='update',
                             subject='Goods',
                             data=subject_id)
                con.commit()

        elif type == 'return':
            quantity = 1
            con = sqlite3.connect(self.database)
            cursor = con.cursor()

            if (cursor.execute(
                    '''select count(*)
                                    from Goods
                                    where Goods.id = ? and Goods.delflg = 1''',
                [subject_id]).fetchone()[0]) == 0:
                print('There wasn\'t sold such goods')
            else:
                try:
                    sql = '''select total from Transactions where Transactions.subject_id = ? 
                    and Transactions.type = \'sell\''''
                    total = cursor.execute(sql, [subject_id]).fetchone()[0]

                    sql = '''insert into Transactions(type, total, subject_id, quantity, date) values(?,?,?,?,?)'''
                    cursor.execute(sql,
                                   [type, total, subject_id, quantity, date])
                    self.add_log(operation='insert',
                                 subject='Transactions',
                                 data=str(type, total, subject_id, quantity,
                                          date))

                    sql = '''update Goods set delflg = 0 where id = ?'''
                    cursor.execute(sql, [subject_id])
                    self.add_log(operation='update',
                                 subject='Goods',
                                 data=str(subject_id))

                    con.commit()

                except Exception as e:
                    print('Error:', e)

        elif type == 'delivery':
            con = sqlite3.connect(self.database)
            cursor = con.cursor()
            sql = '''insert into Transactions(type, total, subject_id, quantity, customer_id, discount, additionalInfo, date) 
                    values(?,?,?,?,?,?,?,?)'''
            data = (type, total, subject_id, quantity, customer_id, discount,
                    additionalInfo, date)
            cursor.execute(sql, data)
            self.add_log(operation='insert',
                         subject='Transactions',
                         data=str(data))
            con.commit()

    def category_sale_add(self,
                          title=None,
                          category_id=0,
                          discount=0,
                          active=0):
        """Adds discount at one of the categories."""
        con = sqlite3.connect(self.database)
        cursor = con.cursor()
        sql = '''insert into Categories_sales(title,category_id,discount,active) values(?,?,?,?)'''
        data = (title, category_id, discount, active)
        cursor.execute(sql, data)
        self.add_log(operation='insert',
                     subject='Categories_sales',
                     data=str(data))
        con.commit()

    def customer_sale_add(self,
                          customer_id=0,
                          title=None,
                          discount=0,
                          active=0):
        """Adds personal discount to the customer."""
        con = sqlite3.connect(self.database)
        cursor = con.cursor()
        sql = '''insert into Customers_sales(customer_id, title, discount, active) values(?,?,?,?)'''
        data = (customer_id, title, discount, active)
        cursor.execute(sql, data)
        self.add_log(operation='insert',
                     subject='Customers_sales',
                     data=str(data))
        con.commit()

    def goods_sell(self, id):
        """Takes good's id, marks it as sold and add a transaction."""

        try:
            con = sqlite3.connect(self.database)
            cursor = con.cursor()
            sql = '''update Goods set delflg = 1 WHERE id = ?'''
            cursor.execute(sql, [id])
            self.add_log(operation='update', subject='Goods', data=str(id))

            # get information about good
            sql = '''select * from Goods where id = ?'''
            result = cursor.execute(sql, [id]).fetchone()
            con.commit()
            self.transactions_add(type='sell', total=result[2], subject_id=id)
        except Exception as e:
            print('An error occurred. Database wasn\'t updated.')
            print('Error:', e)

    def revenue_stat(self,
                     start_date=pd.datetime.now().date() -
                     pd.Timedelta('30 days'),
                     end_date=pd.datetime.now().date()):
        """
        Print statistics between two dates by days.

        Parameters:
        -----------
            start_date: First day of period. As default value set today - 30 days
            end_date: Last day of periods. Today as default.

        """
        income = 0
        outcome = 0
        con = sqlite3.connect(self.database)
        cursor = con.cursor()
        result = cursor.execute(
            '''select id,type,total,date from transactions where date between ? and ?''',
            [start_date, end_date]).fetchall()
        con.commit()

        # Convert to dataframe
        df = pd.DataFrame(result, columns=['id', 'type', 'total', 'date'])

        # Set sign, depends of operations type
        counter = 0
        for elem in df['type']:
            if (elem == 'delivery') or (elem == 'return'):
                df.iloc[counter, 2] *= -1
            counter += 1

        # Print statistics by date
        print(df[['date', 'total']].groupby(by='date').sum())

        # Calculate in- and outcome
        for elem in df.groupby(by='date').sum()['total']:
            if elem >= 0:
                income += elem
            else:
                outcome += elem

        print('Income:', income, 'Outcome:', outcome, 'Balance:',
              income - abs(outcome))

    def user_stat(self,
                  start_date=pd.datetime.now().date() -
                  pd.Timedelta('30 days'),
                  end_date=pd.datetime.now().date(),
                  level=0.2):
        """
        Print size of an average transaction between two dates by customer.

        Parameters:
        -----------
            start_date: First day of period. As default value set today - 30 days
            end_date: Last day of periods. Today as default.
            level : threshold of customers with low activity.

        """

        con = sqlite3.connect(self.database)
        cursor = con.cursor()
        result = cursor.execute(
            '''select customer_id, date from transactions 
                                    where date between ? and ? and type = 'sell\' order by date''',
            [start_date, end_date]).fetchall()

        con.commit()

        df = pd.DataFrame(result, columns=['customer_id', 'date'])
        df.index = range(len(df.index))

        avg_transactions = df['customer_id'].groupby(by=df.date).count().mean()
        print('Avg transactions pro customer: ', avg_transactions)

        data = df.groupby(by=['customer_id']).count()
        data.reset_index(inplace=True)

        print('Customers with 20% of average transactions:')
        data = data.rename(columns={"date": "orders"})

        if len(data[data.orders < avg_transactions * level]) > 0:
            print(data[data.orders < avg_transactions * level])
        else:
            print('No such users or enough data')

    def add_log(self, operation='Unknown', subject='Unknown', data='Unknown'):
        """Add all type, table and query of executed operations in logging database."""

        con = sqlite3.connect(self.log_db)
        cursor = con.cursor()

        sql = '''insert into log_table(operation,subject,data) values(?,?,?)'''
        data = (operation, subject, data)
        cursor.execute(sql, data)
        con.commit()

    @staticmethod
    def dbname_check(name):
        """Database name must consists of alphabetic characters and has '.db' at the end"""

        if name[:-3].isalpha() is True:
            if name[-3:] == '.db':
                db_name = name
            if name.isalpha() is True:
                db_name = name + '.db'
        else:
            db_name = 'dbo.db'
            print(
                'Database name is incorrect or not specified, set to default: dbo.db'
            )
        return db_name

    @staticmethod
    def import_converting(df, columns_number, columns_list, sep=','):
        """
        In depends on the number of columns split string into required fields.
        This is necessary in order to avoid the error caused by the separator in the name of the product or category.

        """

        converted_df = pd.DataFrame(columns=columns_list)
        i = 0
        while i < df.count()[0]:
            if df.iloc[i, 0].count(sep) < columns_number - 1:
                print('Error:', df.iloc[i, 0])
            if df.iloc[i, 0].count(sep) >= columns_number - 1:

                # Search all substrings(sep) in strings(rows):
                sep_position = [
                    pos for pos in range(len(df.iloc[i, 0]))
                    if df.iloc[i, 0].startswith(sep, pos)
                ]

                if columns_number == 3:
                    # Split data(chars) to columns :
                    #   id:             to 1. separator
                    #   title:          between 1. and 2. sep-s
                    #   description:    from 2. to end
                    converted_df = converted_df.append(
                        {
                            'id':
                            df.iloc[i, 0][0:sep_position[0]],
                            'title':
                            df.iloc[i, 0][sep_position[0] + 1:sep_position[1]],
                            'description':
                            df.iloc[i, 0][sep_position[1] + 1:]
                        },
                        ignore_index=True)
                if columns_number == 4:
                    # Split data(chars) to columns :
                    #   id:             to 1. separator
                    #   title:          between 1. and 2. sep-s
                    #   price:          between 2. and 3. sep-s
                    #   categoryId:          from 2. to end
                    converted_df = converted_df.append(
                        {
                            'id':
                            df.iloc[i, 0][0:sep_position[0]],
                            'title':
                            df.iloc[i, 0][sep_position[0] + 1:sep_position[1]],
                            'price':
                            df.iloc[i, 0][sep_position[1] + 1:sep_position[2]],
                            'categoryId':
                            df.iloc[i, 0][sep_position[2] + 1:]
                        },
                        ignore_index=True)
                if columns_number == 5:
                    # Split data(chars) to columns :
                    #   id:             to 1. separator
                    #   first_name:     between 1. and 2. sep-s
                    #   last_name:      between 2. and 3. sep-s
                    #   email:          between 2. and 3. sep-s
                    #   email:          between 3. to end
                    converted_df = converted_df.append(
                        {
                            'id':
                            df.iloc[i, 0][0:sep_position[0]],
                            'first_name':
                            df.iloc[i, 0][sep_position[0] + 1:sep_position[1]],
                            'last_name':
                            df.iloc[i, 0][sep_position[1] + 1:sep_position[2]],
                            'email':
                            df.iloc[i, 0][sep_position[2] + 1:sep_position[3]],
                            'gender':
                            df.iloc[i, 0][sep_position[3] + 1:]
                        },
                        ignore_index=True)
                i += 1
        return converted_df

    @staticmethod
    def customers_validation(df):
        """Validate dataframe according to types of Goods table in database.  Return checked validated dataframe."""

        df['additionalInfo'] = ''
        i = 0
        exception_counter = 0
        while i < df.count()[0]:
            try:
                if not df.iloc[i, 0].isnumeric():
                    print('Table: Customers. Error with id in row:', i,
                          'It will be deleted.')
                    df = df.drop(i + exception_counter, axis=0)
                    exception_counter += 1

                for word in df.iloc[i, 1].split(sep=' '):
                    if not word.isalpha():
                        print(
                            'Table: Customers. Error with first name in row:',
                            i, 'It will be deleted.')
                        df = df.drop(i + exception_counter, axis=0)
                        exception_counter += 1

                for word in df.iloc[i, 2].split(sep=' '):
                    if not word.isalpha():
                        print('Table: Customers. Error with last name in row:',
                              i, 'It will be deleted.')
                        df = df.drop(i + exception_counter, axis=0)
                        exception_counter += 1

                if (df.iloc[i, 3].find('@')
                        < 1) or (df.iloc[i, 3].find('.')
                                 < 3) or not (df.iloc[i, 3][-1].isalpha()):
                    print('Table: Customers. Incorrect email in row:', i,
                          'was moved to additional info.')
                    df.additionalInfo[i + exception_counter] = df.email[
                        i + exception_counter]
                    df.email[i + exception_counter] = ''

                if not (df.iloc[i, 4].lower() == 'male'
                        or df.iloc[i, 4].lower() == 'female'):
                    print('Table: Persons. Error with gender in row:', i,
                          'It will be deleted.')
                    df = df.drop(i + exception_counter, axis=0)
                    exception_counter += 1

                i += 1
            except Exception:
                i += 1
        print(exception_counter, 'rows were deleted')
        return df

    @staticmethod
    def goods_validation(df):
        """Validate dataframe according to types of Goods table in database.  Return checked validated dataframe."""
        i = 0
        exception_counter = 0
        while i < df.count()[0]:
            try:
                if not df.iloc[i, 0].isnumeric():
                    print('Table: Goods. Error with id in row:', i,
                          'It will be deleted.')
                    df = df.drop(i + exception_counter, axis=0)
                    exception_counter += 1
                if df.iloc[i, 1] == '':
                    print('Table: Goods. Error with title in row:', i,
                          'It will be deleted.')
                    df = df.drop(i + exception_counter, axis=0)
                    exception_counter += 1
                if not df.iloc[i, 2].isnumeric():
                    print('Table: Goods. Error with price in row:', i,
                          'It will be deleted.')
                    df = df.drop(i + exception_counter, axis=0)
                    exception_counter += 1
                if not df.iloc[i, 3].isnumeric():
                    print('Table: Goods. Error with category_id in row:', i,
                          'It will be deleted.')
                    df = df.drop(i + exception_counter, axis=0)
                    exception_counter += 1
                i += 1
            except:
                pass
        print(exception_counter, 'rows were deleted')
        return df

    @staticmethod
    def category_validation(df):
        """ Validate counts and types of rows to insert into Category table"""

        i = 0
        exception_counter = 0
        while i < df.count()[0]:
            try:
                if not df.iloc[i, 0].isnumeric():
                    print('Table: Categories. Error with id in row:', i,
                          'It will be deleted.')
                    df = df.drop(i, axis=0)
                    break
                for word in df.iloc[i, 1].split(sep=' '):
                    if not word.isalnum():
                        print('Table: Categories. Error with title in row:', i,
                              'It will be deleted.')
                        df = df.drop(i, axis=0)
                        break
                for word in df.iloc[i, 2].split(sep=' '):
                    if not word.isalnum():
                        print(
                            'Table: Categories. Error with description in row:',
                            i, 'It will be deleted.')
                        df = df.drop(i, axis=0)
                        break
            except:
                pass
        print(exception_counter, 'rows were deleted')
        return df

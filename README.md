my_orm
======

Mini ORM for MySQL

Requirements
============

1. Python >= 2.7
2. MySQL-python >= 1.2.5

Install
=======

1. Download source from git.
2. cd my_orm
3. python setup.py install
4. pip install -r requirements.txt

Description
===========

Mini ORM to use with MySQL
Example:

    from datetime import datetime
    from my_orm import Connect
    from my_orm.dialect import Select

    database = Connect('mysql://guest:Blu3.top@localhost:3306/test_db')

    users = database.users

    result = Select(users.id, users.name)\
        .From(users)\
        .Where(users.created_at >= datetime(2014, 05, 20))

    for row in result:
        print('User id: %s, name: %s' % (row[0], row['name']))


Enjoy it!

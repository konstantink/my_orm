# coding=UTF-8


__author__ = 'Konstantin Kolesnikov'


from datetime import datetime

from my_orm import Connect
from my_orm.dialect import Select, Update



url_str = 'mysql://guest:Blu3.top@localhost:3306/test_db'

# url_obj = url.make_url(url_str)
# print(url_obj)
# print(url_obj.to_connection_args())

conn = Connect(url_str)
# print(conn.users.id)
# print(conn)

users = conn.users
posts = conn.posts

# result = Select(posts.id, posts.user_id)\
#     .From(posts)\
#     .Where(posts.created_at >= datetime(2014, 05, 20))

print(users.name)

result = Select(users.id, users.name)\
    .From(users)\
    .Where(users.created_at >= datetime(2014, 05, 20))

print(result.count())
print(result[0][0])

for row in result:
    print('User id: %s, name: %s' % (row[0], row['name']))

now = datetime.now()
result = Update(users)\
    .Set(last_login=now)\
    .Where(users.id==result[0][0])\
    .Or(users.id==result[3][0])
print(result)
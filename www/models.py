import time,uuid

from orm import Model,StringField,BooleanField,TextField,IntegerField,FloatField

# 生成一个唯一标识uuid作为主键
def next_id():
    return '%015d%s000' % (int(time.time() * 1000), uuid.uuid4().hex)

class User(Model):
    __table__ = 'users'

    id = StringField(primary_key=True, default=next_id, col_type='varchar(50)')
    email = StringField(col_type='varchar(50)')
    passwd = StringField(col_type='varchar(50)')
    admin = BooleanField()
    name = StringField(col_type='varchar(50)')
    image = StringField(col_type='varchar(500)')
    created_at = FloatField(default=time.time)

class Blog(Model):
    __table__ = 'blogs'

    id = StringField(primary_key=True, default=next_id, col_type='varchar(50)')
    user_id = StringField(col_type='varchar(50)')
    user_name = StringField(col_type='varchar(50)')
    user_image = StringField(col_type='varchar(500)')
    name = StringField(col_type='varchar(50)')
    summary = StringField(col_type='varchar(200)')
    content = TextField()
    created_at = FloatField(default=time.time)

class Comment(Model):
    __table__ = 'comments'

    id = StringField(primary_key=True, default=next_id, col_type='varchar(50)')
    blog_id = StringField(col_type='varchar(50)')
    user_id = StringField(col_type='varchar(50)')
    user_name = StringField(col_type='varchar(50)')
    user_image = StringField(col_type='varchar(500)')
    content = TextField()
    created_at = FloatField(default=time.time)
    
if __name__ == "__main__":
    print(next_id(),type(next_id()))
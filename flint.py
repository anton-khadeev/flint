# перед запуском в окне дос поменяй кодировку: chcp 1251

import feedparser
import sqlite3
import time
   
# Global constants
date_format = "%a, %d %b %Y %H:%M:%S"
debug = True

# helper functions

def div_comment(fent):
	""" 
	Выделение ключевых полей из записи rss-флибусты. 
	Запись имеет  вид: [nick] про [book_author]: [book_title]\n[comment] 
	"""
	# ret = [nick, book_author, book_title, comment]
	ret = []

	# nick
	i = 0
	while i < len(fent) and not fent[i:i+5] is " про ":
	    i += 1
	ret.append(fent[:i])
	# print ("[0:",i,"] ",fent[:i])
	
	# book_author 
	i += 5
	j = i
	while i < len(fent) and not fent[i:i+1] is ": ":
	    i += 1
	ret.append(fent[j:i])
	# print ("[",j,":",i,"] ",fent[j:i])
	
	# book_title
	i += 2
	j = i
	while i < len(fent) and not fent[i] is "\n" :
	    i += 1
	ret.append(fent[j:i])
	# print ("[",j,":",i,"] ",fent[j:i])
	
	# comment
	ret.append(fent[i+1:])
	# print ("[",i+1,":] ",fent[i+1:])

	return ret    

def is_table_exist(table_list, table_name):
    for c in table_list:
        if c[0] is table_name:
            return True
    else:
        return False

# time_sctruct -> string
def date2str(date):
	return time.strftime(date_format,date)

# string -> time_sctruct
def str2date(str):
    return time.strptime(str,date_format)

# 1970/01/01 00:00:00
def zero_time():
    return time.localtime(0)

def print_log(s):
	print (s)
	if debug:
	    f.write(s)
	
# Main code

if debug:
    f = open('flint.log', "a")

print_log("\n\n------------------------------------------------------------------------------\n%s\n" % date2str(time.localtime()))

# База данных
db = sqlite3.connect('flint.sqlite3')
cursor = db.cursor()

# Здесь лучше try...catch будет использовать
cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='rssfeed' OR name ='lastdate'")
table_list = cursor.fetchall()

if not ('rssfeed',) in table_list:
    print_log("Таблица rssfeed отсутствовала. Добавлена.\n")
    cursor.execute('CREATE TABLE rssfeed (book_author, book_title, nick, comment, published, link)')

if ('lastdate',) in table_list:
    cursor.execute("SELECT lastdate FROM lastdate")
    
    # Просто проверка на тот случай, если записей будет >1. Вообще не должно быть.
    datesall = cursor.fetchall() 
    
    if len(datesall) > 0:
	    dates = []
	    for i in datesall:
	        dates.append(str2date(i[0]))

	    dates.sort()
	    startdate = dates[-1]
	    # ToDo: убрать лишние записи - должна быть только одна
    else:
        startdate = zero_time()
else:
    print_log("Таблица lastdate отсутствовала. Добавлена.\n")
    cursor.execute('CREATE TABLE lastdate (lastdate)')
    startdate = zero_time()

# Парсинг ленты новостей Флибусты
frss = feedparser.parse('http://flibusta.net/polka/show/all/rss')

imported_counter = 0
lastdate = startdate

# Сохранение новых записей в базу данных
for fent in frss.entries:
	if time.mktime(fent.published_parsed) > time.mktime(startdate):
		# Выделение ключевых полей
		# ret = [nick, book_author, book_title, comment]
		ret = div_comment(fent.description)
		
		# Дата преобразуется в что-то вроде "Thu, 17 Apr 2014 17:41:09". В принципе, можно сразу брать строковую переменную
		# из .published, но там немного по другому: "Thu, 17 Apr 2014 17:41:09 GMT"
		ret.append(date2str(fent.published_parsed))
		
		# Ссылка на книгу на флибусте. По-хорошему, надо убираеть её в библиотеку книг.
		ret.append(fent.link)

		cursor.execute("INSERT INTO rssfeed(nick, book_author, book_title, comment, published, link) values(?,?,?,?,?,?)", ret )
		
		imported_counter += 1
		if fent.published_parsed > lastdate:
		    lastdate = fent.published_parsed

print_log('Новых записей: %s.\n' % imported_counter)

if imported_counter > 0:
    # cursor.execute('DROP TABLE lastdate')
    cursor.execute('INSERT INTO lastdate(lastdate) values(?)',[date2str(lastdate)])

db.commit()
db.close()

if debug:
    if not f.closed:
        f.close()
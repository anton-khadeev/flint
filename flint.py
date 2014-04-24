# перед запуском в окне дос поменяй кодировку: chcp 1251

import feedparser
import sqlite3
import time

date_format = "%a, %d %b %Y %H:%M:%S"

# Helper functions

# Переменная, которая используется для вывода отладочной информации
debug = True

def print_log(s):
    if debug:
        print (s)

# time_sctruct -> string
def date2str(date):
	return time.strftime(date_format,date)

# string -> time_sctruct
def str2date(str):
    return time.strptime(str,date_format)

# 1970/01/01 00:00:00
def zero_time():
    return time.localtime(0)

# Class definition

class Flint(object):
    """
    Класс Flint
    """
    
    def __init__(self):
        """
        Инициализация, проверка базы данных на наличие таблиц и их добавление при необходимости.
        """

        print_log("\n\n------------------------------------------------------------------------------\n")
        print_log("Кэп Флинт: новые впечатления с Флибусты.\n")

        self.rss_feed = 'http://flibusta.net/polka/show/all/rss'
        self.debug    = True        

        # Подключение к БД
        self.db = sqlite3.connect('flint.sqlite3')
        cursor  = self.db.cursor()

		# Выборка таблиц
		# Здесь лучше try...catch будет использовать
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='rssfeed' OR name ='down_date'")
        table_list = cursor.fetchall()

		# Таблица с записями
        if not ('rssfeed',) in table_list:
            print_log("Таблица rssfeed отсутствовала. Добавлена.\n")
            cursor.execute('CREATE TABLE rssfeed (book_author, book_title, nick, comment, published, link)')

		# Таблица с датой последнего сообщения
        if ('down_date',) in table_list:
            cursor.execute("SELECT down_date FROM down_date")
		    
		    # Просто проверка на тот случай, если записей будет >1. Вообще не должно быть.
            datesall = cursor.fetchall() 
		    
            if len(datesall) == 1:
                # Из таблицы извлечена запись с меткой времени
                self.down_date = str2date(datesall[0][0])
                print_log("Дата последнего обновления извлечена из БД: %s.\n" % date2str(self.down_date))            
            elif len(datesall) > 1:
                # В таблице содержатся лишние метки времени
                datesall_sorted = []
                for i in datesall:
                    datesall_sorted.append(str2date(i[0]))                
                datesall_sorted.sort()
                self.down_date = datesall_sorted[-1]
                cursor.execute("DELETE FROM down_date WHERE NOT (down_date = ?)", (date2str(self.down_date),))
            else:
                # В таблице нет меток времени
                self.down_date = zero_time()
                print_log("В БД отсутствует дата последнего обновления. Взята нулевая отметка %s.\n" % date2str(self.down_date))		    
                cursor.execute("INSERT INTO down_date(down_date) values(?)", (date2str(self.down_date),))
        else:
            cursor.execute('CREATE TABLE down_date (down_date)')
            self.down_date = zero_time()
            print_log("Таблица down_date отсутствовала. Добавлена.\n")
            print_log("Взята нулевая отметка %s.\n" % date2str(self.down_date))
            cursor.execute("INSERT INTO down_date(down_date) values(?)", (date2str(self.down_date),))

    def __repr__(self):
	    """
	    """ 
	    return date2str(self.down_date)

    def div_comment(self, fent):
        """ 
        Выделение ключевых полей из записи rss-флибусты. 
        Запись имеет  вид: [nick] про [book_author]: [book_title]\n[comment] 
        """
        # ret = [nick, book_author, book_title, comment
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

    def update(self):
        """
        Обновление записей из RSS-потока.
        """
        
        cursor = self.db.cursor()
        
        # Парсинг ленты новостей Флибусты
        frss = feedparser.parse(self.rss_feed)
        
        imported_counter = 0
        mt_down_date = time.mktime(self.down_date)
        
        # Сохранение новых записей в базу данных
        for fent in frss.entries:
            if time.mktime(fent.published_parsed) > mt_down_date:
                # Выделение ключевых полей
                # ret = [nick, book_author, book_title, comment]
                ret = self.div_comment(fent.description)
                
                # Дата преобразуется в что-то вроде "Thu, 17 Apr 2014 17:41:09". В принципе, можно сразу брать строковую переменную
                # из .published, но там немного по другому: "Thu, 17 Apr 2014 17:41:09 GMT"
                ret.append(date2str(fent.published_parsed))
                
                # Ссылка на книгу на флибусте. По-хорошему, надо убираеть её в библиотеку книг.
                ret.append(fent.link)
                
                cursor.execute("INSERT INTO rssfeed(nick, book_author, book_title, comment, published, link) values(?,?,?,?,?,?)", ret )
                
                if fent.published_parsed > self.down_date:
                    self.down_date = fent.published_parsed

                imported_counter += 1
                
        print_log('Новых записей: %s.\n' % imported_counter)
        
        if imported_counter > 0:
            # cursor.execute('DROP TABLE lastdate')
            cursor.execute('UPDATE down_date SET down_date = ?',[date2str(self.down_date)])

        self.db.commit()

    def close(self):
        self.db.close()

# Основная процедура
f = Flint()
f.update()
f.close()

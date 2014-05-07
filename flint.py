# -*- coding: utf-8 -*-

# Замечания: перед запуском в окне дос поменяй кодировку: chcp 1251

import feedparser
import sqlite3
import time
import re

# Helper functions

date_format = "%d %b %Y %H:%M:%S"   # predefined format

def date2str(date):
    """ Convert 'time_sctruct' -> 'string' with predefined format """
    return time.strftime(date_format, date)

def str2date(str):
    """ Convert 'string' -> 'time_sctruct' with predefined format """
    return time.strptime(str,date_format)

def zero_time():
    """ Starting point of machine time: 1970/01/01 00:00:00 """
    return time.localtime(0)

# Class definition

class Flint(object):
    """ Last impressions from www.flibusta.net """
    
    def __init__(self, water_line):
        self.rss_feed    = 'http://flibusta.net/polka/show/all/rss'
        self.water_line  = water_line
        self.loot        = []

    def __repr__(self):
	    return "Flint: "+ str(len(self.loot)) + " entries, last from " + date2str(self.water_line)

    def __str__(self):
        return "Flint: "+ str(len(self.loot)) + " entries, last from " + date2str(self.water_line)

    def entry_interp(self, entry):
        """ Regexp to entry: [nick] про [book_author]: [book_title]\n[comment] """

        p = re.compile(r"(?P<nick>[ .0-9a-zA-Zа-яёА-ЯЁ_-]+) про (?P<author>.+): (?P<book>.+)\n(?P<comment>(?:.|\n)*)")
        
        r = p.search(entry)
        
        if not r is None:
            ret = dict()    # ret = [nick, book_author, book_title, comment]

            ret["nick"]    = r.group("nick")
            ret["author"]  = r.group("author")
            ret["book"]    = r.group("book")
            ret["comment"] = r.group("comment")

            return ret    
        else:
            return None

    def update(self):
        """ Recieve new entries """
        
        frss = feedparser.parse(self.rss_feed)          # Парсинг ленты новостей Флибусты
        
        mt_down_date = time.mktime(self.water_line)     # Довольно неуклюжий способ сравнения двух дат,
                                                        # надеюсь, позже придумаю лучше.       
        for ent in frss.entries:
            if time.mktime(ent.published_parsed) > mt_down_date:
                ret = self.entry_interp(ent.description)
                if not ret is None:

                    # published -> '17 Apr 2014 17:41:09' (instead of 'Thu, 17 Apr 2014 17:41:09 GMT')
                    ret["published"] = date2str(ent.published_parsed)
                    ret["link"]      = ent.link

                    self.loot.append(ret)

                    if self.water_line < ent.published_parsed:
                        self.water_line = ent.published_parsed
                else:
                    print ("Проблема с распознаванием записи\n'"+ent.description+'\nЗапись пропущена.')

class Hold(object):
    """ Holder for recieved entries from Flint """
    
    def __init__(self):
        """ Check db on table exist """
        
        self.db = sqlite3.connect('flint.sqlite3')
        cursor  = self.db.cursor()

        # Extraction of all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='hold' OR name ='config'")
        table_list = cursor.fetchall()

        # Entries
        if not ('hold',) in table_list:
            cursor.execute('CREATE TABLE hold (nick, author, book, comment, published, link)')
            print ('В БД отсутствовала таблица hold. Добавлена.')

        # last date
        if not ('config',) in table_list:
            cursor.execute('CREATE TABLE config (water_line)')
            cursor.execute("INSERT INTO config(water_line) values(?)", (date2str(zero_time()),))
            print ('В БД отсутствовала таблица config. Добавлена.')
        else:
            cursor.execute("SELECT water_line FROM config")
            datesall = cursor.fetchall() 
            
            if len(datesall) > 1:
                datesall_sorted = []
                for i in datesall:
                    datesall_sorted.append(str2date(i[0]))                
                datesall_sorted.sort()
                self.water_line = datesall_sorted[-1]
                cursor.execute("DELETE FROM config WHERE NOT (water_line = ?)", (date2str(self.water_line),))
            elif len(datesall) == 0:
                cursor.execute("INSERT INTO config (water_line) values(?)", (date2str(zero_time()),))

        self.db.commit()

    def __repr__(self):
        return "Hold: flint.sqlite3"

    def __str__(self):
        return "Hold: flint.sqlite3"

    def read(self):
        pass

    def save(self, loot):
        cursor  = self.db.cursor()
        for e in loot:
            cursor.execute("INSERT INTO hold (nick, author, book, comment, published, link) \
            values(?,?,?,?,?,?)", (e["nick"], e["author"], e["book"], e["comment"], e["published"], e["link"]) )
        self.db.commit()

    def close(self):
        """ Close DB handler """
        self.db.close()

    def get_last_published(self):
        """ Date of latest record from table. It takes from field 'published'"""       
        cursor  = self.db.cursor()
        cursor.execute("SELECT published FROM hold")       
        datesall = cursor.fetchall()
        datesall.sort()
        return str2date(datesall[-1][0])

    def get_water_line(self):
        """ Read saved mark of last recieved data """
        cursor  = self.db.cursor()
        cursor.execute("SELECT water_line FROM config")
        
        datesall = cursor.fetchall() 
        
        if len(datesall) == 1:
            return str2date(datesall[0][0])
        elif len(datesall) > 1:
            datesall_sorted = []
            for i in datesall:
                datesall_sorted.append(str2date(i[0]))                
            datesall_sorted.sort()
            return datesall_sorted[-1]
        else:
            # В таблице нет меток времени
            return None

    def update_water_line(self, water_line):
        cursor  = self.db.cursor()
        cursor.execute('UPDATE config SET water_line = ?',(date2str(water_line),))
        self.db.commit()


# Test

# f = Flint(zero_time())
# print (f)
# print ()

# f.update()
# print (f)
# print ()
# print (f.loot[-1])
# print ()

# h = Hold()
# print (h)
# print (date2str(h.get_water_line()))

# h.save(f.loot)

# ld = h.get_last_published()
# h.update_water_line(ld)
# print(date2str(ld))

# h.close()

from tkinter import filedialog
import pandas as pd
import sqlite3
import os
import shutil
from pathlib import Path


def open_doplata_file():
    global file_doplata
    global fd
    file_doplata = filedialog.askopenfilename(title="Выберите файл доплат", filetypes=[("Файлы Excel", "*.xls")])
    fd = open(file_doplata, encoding="cp866")
    fd.read()


def open_journal_file():
    global file_journal
    global fj
    file_journal = filedialog.askopenfilename(title="Выберите файл журнала", filetypes=[("Файлы Excel", "*.xls")])
    fj = open(file_journal, encoding="cp866")
    fj.read()


def open_index_file():
    global file_index
    global fi
    file_index = filedialog.askopenfilename(title="Выберите файл INDEX", filetypes=[("Файлы Excel", "*.xls")])
    fi = open(file_index, encoding="cp866")
    fi.read()


def set_dataframes():
    global dataframe_doplata
    global dataframe_journal
    global dataframe_index

    dataframe_doplata = pd.read_excel(file_doplata, "Report", skiprows=4, names=['FIORDAT',
                                                                                 'NPERS',
                                                                                 'RN',
                                                                                 'OSN',
                                                                                 'SUMDOP',
                                                                                 'SUMRAZDOP',
                                                                                 'SUMM',
                                                                                 'DATOBR',
                                                                                 'DATRESH',
                                                                                 'OPERATION',
                                                                                 'PERDOP',
                                                                                 'SPOSOB',
                                                                                 'NOMDOC'])
    dataframe_journal = pd.read_excel(file_journal, "reportPD", names=['№',
                                                                       'NPERS',
                                                                       'FIOPENS',
                                                                       'FIOPOL',
                                                                       'OPERATION',
                                                                       'PERDOP',
                                                                       'USTRAZPENS',
                                                                       'DOPPOPEN',
                                                                       'EDV',
                                                                       'DOPPOEDV',
                                                                       'DMO',
                                                                       'DOPDMO',
                                                                       'SUMM',
                                                                       'DOSTORG',
                                                                       'SPEC',
                                                                       'PRIM',
                                                                       'DAT'])

    dataframe_index = pd.read_excel(file_index, "INDEX4", names=['RA',
                                                                 'NPERS',
                                                                 'FIO',
                                                                 'RDAT',
                                                                 'VIDVIPL',
                                                                 'DNAZ',
                                                                 'FVDO',
                                                                 'GPO1DO',
                                                                 'GPO2DO',
                                                                 'SCHDO',
                                                                 'FVPO',
                                                                 'GPO1PO',
                                                                 'GPO2PO',
                                                                 'SHCPO',
                                                                 'IZMPEN',
                                                                 'DOPDO',
                                                                 'DOPPO',
                                                                 'IZMDOP',
                                                                 'RABDO',
                                                                 'RABPO',
                                                                 'VIPLATA'])


def db_ops():
    if os.path.isfile('tmp/tmp.db'):
        os.remove('tmp/tmp.db')

    con = sqlite3.connect('tmp/tmp.db')
    cur = con.cursor()

    cur.execute('CREATE TABLE DOPLATA ('
                'FIORDAT, '
                'NPERS, '
                'RN, '
                'OSN, '
                'SUMDOP, '
                'SUMRAZDOP, '
                'SUMM, '
                'DATOBR, '
                'DATRESH, '
                'OPERATION, '
                'PERDOP, '
                'SPOSOB, '
                'NOMDOC)')

    cur.execute('CREATE TABLE JOURNAL ('
                '№, '
                'NPERS, '
                'FIOPENS, '
                'FIOPOL, '
                'OPERATION, '
                'PERDOP, '
                'USTRAZPENS, '
                'DOPPOPEN, '
                'EDV, '
                'DOPPOEDV, '
                'DMO, '
                'DOPDMO, '
                'SUMM, '
                'DOSTORG, '
                'SPEC, '
                'PRIM, '
                'DAT)')

    cur.execute('CREATE TABLE INDEX4 ('
                'RA, '
                'NPERS, '
                'FIO, '
                'RDAT, '
                'VIDVIPL, '
                'DNAZ, '
                'FVDO, '
                'GPO1DO, '
                'GPO2DO, '
                'SCHDO, '
                'FVPO, '
                'GPO1PO, '
                'GPO2PO, '
                'SHCPO, '
                'IZMPEN, '
                'DOPDO, '
                'DOPPO, '
                'IZMDOP, '
                'RABDO, '
                'RABPO, '
                'VIPLATA)')

    dataframe_doplata.to_sql("DOPLATA", con, if_exists="append", index=False)
    dataframe_journal.to_sql("JOURNAL", con, if_exists="append", index=False)
    dataframe_index.to_sql("INDEX4", con, if_exists="append", index=False)

    cur.execute("UPDATE JOURNAL\n"
                "SET SUMM = REPLACE(SUMM, '.', ',');")

    cur.execute("UPDATE DOPLATA\n"
                "SET SUMM = REPLACE(SUMM, '.', ',');")

    cur.execute("UPDATE INDEX4\n"
                "SET DOPPO = REPLACE(DOPPO, '.', ',');")

    cur.execute('DELETE FROM JOURNAL\n'
                'WHERE rowid NOT IN (\n'
                    'SELECT MIN(rowid)\n'
                    'FROM JOURNAL\n'
                    'GROUP BY NPERS);')

    cur.execute('CREATE TABLE SPISOK AS\n'
                'SELECT J.FIOPENS, J.NPERS, IFNULL(J.SUMM, 0) AS SUMM, IFNULL(D.SUMM, 0) AS DOPSUMM, IFNULL(I.DOPPO, 0) AS DOPPO, J.DOSTORG\n'
                'FROM JOURNAL J\n'
                'LEFT JOIN DOPLATA D USING(NPERS)\n'
                'LEFT JOIN INDEX4 I USING(NPERS)\n'
                'UNION\n'
                'SELECT D.FIORDAT, D.NPERS, IFNULL(J.SUMM, 0) AS SUMM, IFNULL(D.SUMM, 0) AS DOPSUMM, IFNULL(I.DOPPO, 0) AS DOPPO, D.SPOSOB\n'
                'FROM DOPLATA D\n'
                'LEFT JOIN JOURNAL J USING(NPERS)\n'
                'LEFT JOIN INDEX4 I USING(NPERS)\n'
                'UNION\n'
                'SELECT I.FIO, I.NPERS, IFNULL(J.SUMM, 0) AS SUMM, IFNULL(D.SUMM, 0) AS DOPSUMM, IFNULL(I.DOPPO, 0) AS DOPPO, I.VIPLATA\n'
                'FROM INDEX4 I\n'
                'LEFT JOIN JOURNAL J USING(NPERS)\n'
                'LEFT JOIN DOPLATA D USING(NPERS);')

    cur.execute('DELETE FROM SPISOK\n'
                'WHERE (SUMM + DOPPO) = DOPSUMM;')

    cur.execute('DELETE FROM SPISOK\n'
                'WHERE rowid NOT IN (\n'
                    'SELECT MIN(rowid)\n'
                    'FROM SPISOK\n'
                    'GROUP BY NPERS);')

    cur.execute('DELETE FROM SPISOK\n'
                'WHERE (SUMM = DOPSUMM) OR (DOPPO = DOPSUMM)')


    dataframe_final = pd.read_sql_query('SELECT FIOPENS as "ФИО",\n '
                                        'NPERS as "СНИЛС",\n '
                                        'SUMM as "СУММА В ЖУРНАЛЕ",\n'
                                        'DOPSUMM as "СУММА ИЗ ПТК НВП",\n'
                                        'DOPPO as "СУММА ИЗ ИНДЕКСА",\n'
                                        'DOSTORG as "ДОСТАВОЧНАЯ ОРГАНИЗАЦИЯ"\n'
                                        'FROM SPISOK', con)

    Path("C:\\out").mkdir(parents=True, exist_ok=True)

    dataframe_final.to_excel("C:\\out\\Список расхождения сумм.xls", index=False)

    con.commit()
    con.close()


def cleaner():
    folder = 'tmp'
    for file in os.listdir(folder):
        file_path = os.path.join(folder, file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
            elif os.path.isfile(file_path):
                shutil.rmtree(file_path)
        except:
            pass


if __name__ == '__main__':
    try:
        open_doplata_file()
        open_journal_file()
        open_index_file()
        set_dataframes()
        db_ops()
    finally:
        cleaner()



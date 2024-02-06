import pymysql
import pandas as pd
import matplotlib.pyplot as plt
import koreanize_matplotlib


def init_table1(conn, cur, database, table, df):
    sqlCreateTable = f"""-- sql
        CREATE TABLE {table} (
            연도 SMALLINT UNSIGNED NOT NULL,
            전국 INT UNSIGNED,
            서울특별시 INT UNSIGNED,
            부산광역시 INT UNSIGNED,
            대구광역시 INT UNSIGNED,
            인천광역시 INT UNSIGNED,
            광주광역시 INT UNSIGNED,
            대전광역시 INT UNSIGNED,
            울산광역시 INT UNSIGNED,
            세종특별자치시 INT UNSIGNED,
            경기도 INT UNSIGNED,
            강원특별자치도 INT UNSIGNED,
            충청북도 INT UNSIGNED,
            충청남도 INT UNSIGNED,
            전북특별자치도 INT UNSIGNED,
            전라남도 INT UNSIGNED,
            경상북도 INT UNSIGNED,
            경상남도 INT UNSIGNED,
            제주특별자치도 INT UNSIGNED,
            CONSTRAINT pk_population PRIMARY KEY (연도)
        )"""
    create_table(conn, cur, database, table, sqlCreateTable)

    for i in range(df.shape[0]):
        sqlInsertData = f"""-- sql
            INSERT INTO {table} (연도, 전국, 서울특별시, 부산광역시, 대구광역시, 인천광역시, 광주광역시, 대전광역시, 울산광역시, 세종특별자치시,
                                경기도, 강원특별자치도, 충청북도, 충청남도, 전북특별자치도, 전라남도, 경상북도, 경상남도, 제주특별자치도)
            VALUES {tuple(df.iloc[i].to_list())}"""
        insert_data(conn, cur, database, table, sqlInsertData)


def init_table2(conn, cur, database, table, df):
    sqlCreateTable = f"""-- sql
        CREATE TABLE {table} (
            연도 SMALLINT UNSIGNED NOT NULL,
            전국 INT UNSIGNED,
            서울특별시 INT UNSIGNED,
            부산광역시 INT UNSIGNED,
            대구광역시 INT UNSIGNED,
            인천광역시 INT UNSIGNED,
            광주광역시 INT UNSIGNED,
            대전광역시 INT UNSIGNED,
            울산광역시 INT UNSIGNED,
            세종특별자치시 INT UNSIGNED,
            경기도 INT UNSIGNED,
            강원특별자치도 INT UNSIGNED,
            충청북도 INT UNSIGNED,
            충청남도 INT UNSIGNED,
            전북특별자치도 INT UNSIGNED,
            전라남도 INT UNSIGNED,
            경상북도 INT UNSIGNED,
            경상남도 INT UNSIGNED,
            제주특별자치도 INT UNSIGNED,
            CONSTRAINT fk_schoolage FOREIGN KEY (연도) REFERENCES population_table (연도)
        )"""
    create_table(conn, cur, database, table, sqlCreateTable)
    
    for i in range(df.shape[0]):
        sqlInsertData = f"""-- sql
            INSERT INTO {table} (연도, 전국, 서울특별시, 부산광역시, 대구광역시, 인천광역시, 광주광역시, 대전광역시, 울산광역시, 세종특별자치시,
                                경기도, 강원특별자치도, 충청북도, 충청남도, 전북특별자치도, 전라남도, 경상북도, 경상남도, 제주특별자치도)
            VALUES {tuple(df.iloc[i].to_list())}"""
        insert_data(conn, cur, database, table, sqlInsertData)


def init_table3(conn, cur, database, table, df):
    sqlCreateTable = f"""-- sql
        CREATE TABLE {table} (
            연도 SMALLINT UNSIGNED NOT NULL,
            출생아수 MEDIUMINT UNSIGNED,
            자연증가건수 MEDIUMINT,
            조출생률 FLOAT(2, 1),
            자연증가율 FLOAT(2, 1),
            합계출산율 FLOAT(4, 3),
            출생성비 FLOAT(4, 1),
            CONSTRAINT fk_birthrate FOREIGN KEY (연도) REFERENCES population_table (연도)
        )"""
    create_table(conn, cur, database, table, sqlCreateTable)

    for i in range(df.shape[0]):
        sqlInsertData = f"""-- sql
            INSERT INTO {table} (연도, 출생아수, 자연증가건수, 조출생률, 자연증가율, 합계출산율, 출생성비)
            VALUES {tuple(df.iloc[i].to_list())}"""
        insert_data(conn, cur, database, table, sqlInsertData)


def init_table4(conn, cur, database, table, df):
    sqlCreateTable = f"""-- sql
        CREATE TABLE {table} (
            연도 SMALLINT UNSIGNED NOT NULL,
            총합 MEDIUMINT UNSIGNED,
            초등학생 MEDIUMINT UNSIGNED,
            중학생 MEDIUMINT UNSIGNED,
            고등학생 MEDIUMINT UNSIGNED,
            대학생 MEDIUMINT UNSIGNED,
            CONSTRAINT fk_schoolage_detail FOREIGN KEY (연도) REFERENCES population_table (연도)
        )"""
    create_table(conn, cur, database, table, sqlCreateTable)

    for i in range(df.shape[0]):
        sqlInsertData = f"""-- sql
            INSERT INTO {table} (연도, 총합, 초등학생, 중학생, 고등학생, 대학생)
            VALUES {tuple(df.iloc[i].to_list())}"""
        insert_data(conn, cur, database, table, sqlInsertData)


def create_table(conn, cur, databaseName, tableName, sqlCreate):
    try:
        sqlUse = f"""-- sql
            USE {databaseName}"""
        sql_fk_checks0 = f"""-- sql
            SET foreign_key_checks = 0"""
        sqlDrop = f"""-- sql
            DROP TABLE IF EXISTS {tableName}"""
        sql_fk_checks1 = f"""-- sql
            SET foreign_key_checks = 1"""

        cur.execute(sqlUse)
        cur.execute(sql_fk_checks0)
        cur.execute(sqlDrop)
        cur.execute(sql_fk_checks1)
        cur.execute(sqlCreate)
        conn.commit()
        print(f"{tableName} Table 생성 완료")
    except Exception as e:
        print(e)  # 에러메세지 출력


def insert_data(conn, cur, databaseName, tableName, sqlInsert):
    try:
        sqlUse = f"""-- sql
            USE {databaseName}"""
        
        cur.execute(sqlUse)
        cur.execute(sqlInsert)
        conn.commit()
        print(f"{tableName} Table 데이터 추가 완료")
    except Exception as e:
        print(e)  # 에러메세지 출력


def join_tables(conn, cur, databaseName, tableName1, tableName2, tableName3, joinName):
    try:
        sqlDrop = f"""-- sql
            DROP VIEW IF EXISTS {joinName}"""
        sqlUse = f"""-- sql
            USE {databaseName}"""
        sqlJoin = f"""-- sql
            CREATE VIEW {joinName} as
            SELECT t1.연도, ROUND(t1.전국 / 10000) as '총 인구수(만명)', t3.합계출산율, ROUND(t2.전국 / 10000) as '학령인구수(만명)'
            FROM {tableName1} as t1
            INNER JOIN {tableName2} as t2
            ON t1.연도 = t2.연도
            INNER JOIN {tableName3} as t3
            ON t2.연도 = t3.연도"""

        cur.execute(sqlDrop)
        cur.execute(sqlUse)
        cur.execute(sqlJoin)
        conn.commit()
    except Exception as e:
        print(e)  # 에러메세지 출력


def table_to_dataframe(cur, databaseName, tableName, *columns, distinct=False, **clause):
    if not columns:
        columns = "*"  # 디폴트는 전체 컬럼
    else:
        columns = ', '.join(columns)  # 가변인수를 문자열로 변환
    if distinct:
        columns = 'DISTINCT ' + columns  # 중복 제거가 필요할 경우 적용
    try:
        sqlUse = f"""-- sql
            USE {databaseName}"""
        # 기본 출력 쿼리문
        sqlDisplay = f"""-- sql
            SELECT {columns} FROM {tableName}"""
        # 키워드 인자에 따라 조건 쿼리문 추가
        if clause:
            for k, v in clause.items():
                if k.lower() == 'where':
                    sqlDisplay += f" WHERE {v}"
                elif k.lower() == 'group_by':
                    sqlDisplay += f" GROUP BY {v}"
                elif k.lower() == 'having':
                    sqlDisplay += f" HAVING {v}"
                elif k.lower() == 'order_by':
                    sqlDisplay += f" ORDER BY {v}"
        # 쿼리문이 제대로 작성되었는지 확인
        # print(sqlDisplay)

        cur.execute(sqlUse)
        cur.execute(sqlDisplay)
        # 조회 결과를 데이터프레임으로 저장
        dataframe = pd.DataFrame(cur.fetchall())
        return dataframe
    except Exception as e:
        print(e)  # 에러메세지 출력


# def draw_plot


def main():
    file_population = '../DATA/행정구역별 인구수 2013-2022.xlsx'
    file_schoolage = '../DATA/행정구역별 학령인구 2013-2022.xlsx'
    file_birthrate = '../DATA/출생아수 합계출산율 자연증가 등 2013-2022.xlsx'

    popultationDF = pd.read_excel(file_population, header=0, index_col=0).T
    popultationDF.columns.name = None
    popultationDF.reset_index(inplace=True)
    popultationDF.rename(columns={'index':'연도'}, inplace=True)
    popultationDF['연도'] = popultationDF['연도'].astype(int)

    schoolageDF = pd.read_excel(file_schoolage, header=0, index_col=[0,1]).T
    schoolageDF = schoolageDF.xs('총합', level=1, axis=1)
    schoolageDF.columns.name = None
    schoolageDF.reset_index(inplace=True)
    schoolageDF.rename(columns={'index':'연도'}, inplace=True)
    schoolageDF['연도'] = schoolageDF['연도'].astype(int)

    birthrateDF = pd.read_excel(file_birthrate, header=0, index_col=0).T
    birthrateDF.columns.name = None
    birthrateDF.reset_index(inplace=True)
    birthrateDF.rename(columns={'index':'연도'}, inplace=True)
    birthrateDF['연도'] = birthrateDF['연도'].astype(int)

    schoolage_detailDF = pd.read_excel(file_schoolage, header=0, index_col=[0,1]).T
    schoolage_detailDF = schoolage_detailDF.xs('전국', level=0, axis=1)
    schoolage_detailDF.columns.name = None
    schoolage_detailDF.reset_index(inplace=True)
    schoolage_detailDF.rename(columns={'index':'연도'}, inplace=True)
    schoolage_detailDF['연도'] = schoolage_detailDF['연도'].astype(int)

    # 데이터베이스 접속
    conn = pymysql.connect(host='localhost', port=3306,
                           user='root', passwd='1234', db='schoolage', charset='utf8')
    # 커서를 딕셔너리 형태로 생성
    cur = conn.cursor(pymysql.cursors.DictCursor)
    
    database = "schoolage"
    population_table = "population_table"
    schoolage_table = "schoolage_table"
    birthrate_table = "birthrate_table"
    schoolage_detail_table = "schoolage_detail_table"

    try:
        cur.execute(f"SELECT * FROM {population_table}")
    except Exception as e:
        init_table1(conn, cur, database, population_table, popultationDF)
    try:
        cur.execute(f"SELECT * FROM {schoolage_table}")
    except Exception as e:
        init_table2(conn, cur, database, schoolage_table, schoolageDF)
    try:
        cur.execute(f"SELECT * FROM {birthrate_table}")
    except Exception as e:
        init_table3(conn, cur, database, birthrate_table, birthrateDF)
    try:
        cur.execute(f"SELECT * FROM {schoolage_detail_table}")
    except Exception as e:
        init_table4(conn, cur, database, schoolage_detail_table, schoolage_detailDF)


    joined_view = "pop_school_view"
    
    join_tables(conn, cur, database, population_table, schoolage_table, birthrate_table, joined_view)
    
    popDF = table_to_dataframe(cur, database, joined_view)

    print(popDF)

    plt.plot(popDF["연도"], popDF["총 인구수(만명)"])
    plt.plot(popDF["연도"], popDF["학령인구수(만명)"])
    plt.xlabel("연도")
    plt.ylabel("총 인구수(만명)")
    plt.xticks(popDF["연도"])
    plt.show()





if __name__ == '__main__':
    main()

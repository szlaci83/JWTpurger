import base64, jwt, time, json, pymysql.cursors, logging
import datetime

properties = json.loads(open('properties.json').read())
logging.basicConfig(filename='token_purger.log', level=logging.DEBUG)

secret = properties['secret']

def isExpired(token):
    decoded = jwt.decode(token, base64.b64decode(secret), options={'verify_exp': False})
    if (decoded['exp'] < time.time()):
        return True
    return False

connection = pymysql.connect(host = properties['host'],
                             user = properties['user'],
                             password = properties['password'],
                             db = properties['db'],
                             charset = 'utf8mb4',
                             cursorclass = pymysql.cursors.DictCursor)

def fetchExpiredTokens():
    try:
        with connection.cursor() as cursor:
            tokens = []
            sql = "SELECT * FROM `tokens`"
            cursor.execute(sql)
            result = cursor.fetchall()
            for row in result:
                if isExpired(row['token']):
                    tokens.append(row['token'])
        connection.commit()
    except RuntimeError:
        logging.error('{} - {}'.format(datetime.datetime.today().strftime('%Y-%m-%d'), 'ERROR during fetching expired tokens'))
        pass
    return tokens

def deleteToken(exp_token):
    try:
        cursor = connection.cursor()
        sql = "DELETE FROM `tokens` WHERE `token` = %s"
        cursor.execute(sql, (exp_token,))
        connection.commit()
    except RuntimeError:
        logging.error('{} - {}'.format(datetime.datetime.today().strftime('%Y-%m-%d'), 'ERROR during deleting expired tokens'))
        pass

def purgeAllExpired():
    tokens = fetchExpiredTokens()
    for token in tokens:
        deleteToken(token)
        logging.info('{} - {}'.format(datetime.datetime.today().strftime('%Y-%m-%d'), 'SUCCESFULLY deleted token:' + str(token)))
    connection.close()
    return str(len(tokens))

if __name__ == "__main__" :
    no_of_purged = purgeAllExpired()
    print("Purged " + no_of_purged + " tokens.")
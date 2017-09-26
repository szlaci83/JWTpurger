import base64, jwt, time, json, pymysql.cursors, logging
import datetime, sys

properties = json.loads(open('properties.json').read())
logging.basicConfig(filename='token_purger.log', level=logging.DEBUG)

secret = properties['secret']

def isExpired(token):
    decoded = jwt.decode(token, base64.b64decode(secret), options={'verify_exp': False})
    if (decoded['exp'] < time.time()):
        return True
    return False

try:
    connection = pymysql.connect(#host = properties['host'],
                            host = '192.145.32.11',
                             user = properties['user'],
                             password = properties['password'],
                             db = properties['db'],
                             charset = 'utf8mb4',
                             cursorclass = pymysql.cursors.DictCursor)
except pymysql.Error:
    logging.error('{} - {}'.format(datetime.datetime.today().strftime('%Y-%m-%d'), 'ERROR during connecting to database, terminated'))
    sys.exit(1) #terminate the program if there is an issue to connect to DB

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
        #logging.info('{} - {}'.format(datetime.datetime.today().strftime('%Y-%m-%d'), 'SUCCESFULLY deleted token:' + str(token)))
    connection.close()
    return str(len(tokens))

if __name__ == "__main__" :
    no_of_purged = purgeAllExpired()
    #print("Purged " + no_of_purged + " tokens.")
    if int(no_of_purged) > 0:
        logging.info('{} - {}'.format(datetime.datetime.today().strftime('%Y-%m-%d'), 'SUCCESFUL -  purged :' + no_of_purged + 'tokens.'))
    else:
        logging.info('{} - {}'.format(datetime.datetime.today().strftime('%Y-%m-%d'), 'SUCCESFUL - no token needs to be purged.'))
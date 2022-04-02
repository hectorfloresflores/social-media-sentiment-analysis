import io
import base64
import os.path
import json
import oci
import logging
from fdk import response
import cx_Oracle
from textblob import TextBlob
import re
import snscrape.modules.twitter as sntwitter
import os
import datetime

USERNAME_SECRET_OCID = "ocid1.vaultsecret.oc1.iad.amaaaaaa6zs4ania3rxe4vtmytpe3ax3ugc3jzughvr4wfblyv6wpgwvlaoa"
PASSWORD_SECRET_OCID = "ocid1.vaultsecret.oc1.iad.amaaaaaa6zs4aniaayohkirdbnbkpwf6xzktjasb3rubrimhfscw7m36zhcq"
DB_URL_SECRET_OCID = "ocid1.vaultsecret.oc1.iad.amaaaaaa6zs4ania6ff2xukhf6isorjzr6petdl4sjdagbcnp22kx6ibaypq"
CWALLET_SECRET_OCID = "ocid1.vaultsecret.oc1.iad.amaaaaaa6zs4aniap7svndvdhee3ksam424fssnxce4jw5uhm5sterpm64lq"
EWALLET_SECRET_OCID = "ocid1.vaultsecret.oc1.iad.amaaaaaa6zs4aniaww5hl5gbnurhfzgtnwfoiennqvcb36rs4c3jpkn4b67a"
KEYSTORE_SECRET_OCID = "ocid1.vaultsecret.oc1.iad.amaaaaaa6zs4ania4jztx35llf3jaoyljebnajw63de3gpy2lb7mvfkxma5a"
TRUSTSTORE_SECRET_OCID = "ocid1.vaultsecret.oc1.iad.amaaaaaa6zs4ania6jbmowuvgxqdqeal3l5zdx3xxdajxhi3d6nsm32mf5fa"
OJDBC_SECRET_OCID = "ocid1.vaultsecret.oc1.iad.amaaaaaa6zs4anianqbiuj6befku4hpkgpw3wtcfhi3mrt2kmjcdcdq5bjfq"
SQLNET_SECRET_OCID = "ocid1.vaultsecret.oc1.iad.amaaaaaa6zs4aniacyechtpl5oklrm4uknuzzwwwvalmzstkd2pw3g2vhe3a"
TNSNAMES_SECRET_OCID = "ocid1.vaultsecret.oc1.iad.amaaaaaa6zs4aniaqjgzlqr4vdp45kovvg7edbd6xupox455y7u6n2ywqj3q"
DB_WALLET_PATH = "/tmp"

db_wallet_dict = {'cwallet.sso': CWALLET_SECRET_OCID,
                  'ewallet.p12': EWALLET_SECRET_OCID,
                  'keystore.jks': KEYSTORE_SECRET_OCID,
                  'truststore.jks': TRUSTSTORE_SECRET_OCID,
                  'ojdbc.properties': OJDBC_SECRET_OCID,
                  'sqlnet.ora': SQLNET_SECRET_OCID,
                  'tnsnames.ora': TNSNAMES_SECRET_OCID
                  }


def point_db_wallet_path():
    try:
        with open(DB_WALLET_PATH + '/sqlnet.ora') as orig_sqlnetora:
            newText = orig_sqlnetora.read().replace('DIRECTORY=\"?/network/admin\"',
                                                    'DIRECTORY=\"{}\"'.format(DB_WALLET_PATH))
        with open(DB_WALLET_PATH + '/sqlnet.ora', "w") as new_sqlnetora:
            new_sqlnetora.write(newText)
    except Exception as err:
        print("ERROR: failed to point db wallet path in sqlnet.ora file...", err, flush=True)
        raise

def write_db_wallet_files():
    try:
        for key, value in db_wallet_dict.items():
            print("filename : ", key)
            print("ocid : ", value)
            get_binary_secret_into_file(value, os.path.join(DB_WALLET_PATH, key))
    except Exception as err:
        print("ERROR: failed to write db wallet files...", err, flush=True)
        raise

def get_text_secret(secret_ocid):
    #decrypted_secret_content = ""
    signer = oci.auth.signers.get_resource_principals_signer()
    try:
        client = oci.secrets.SecretsClient({}, signer=signer)
        secret_content = client.get_secret_bundle(secret_ocid).data.secret_bundle_content.content.encode('utf-8')
        decrypted_secret_content = base64.b64decode(secret_content).decode("utf-8")
    except Exception as ex:
        print("ERROR: failed to retrieve the secret content", ex, flush=True)
        raise
    return decrypted_secret_content

def get_binary_secret_into_file(secret_ocid, filepath):
    #decrypted_secret_content = ""
    signer = oci.auth.signers.get_resource_principals_signer()
    try:
        client = oci.secrets.SecretsClient({}, signer=signer)
        secret_content = client.get_secret_bundle(secret_ocid).data.secret_bundle_content.content.encode('utf-8')
    except Exception as ex:
        print("ERROR: failed to retrieve the secret content", ex, flush=True)
        raise
    try:
        with open(filepath, 'wb') as secretfile:
            decrypted_secret_content = base64.decodebytes(secret_content)
            secretfile.write(decrypted_secret_content)
    except Exception as ex:
        print("ERROR: cannot write to file " + filepath, ex, flush=True)
        raise


def connect_to_db():
    username = get_text_secret(USERNAME_SECRET_OCID)
    logging.getLogger().info("username : " + username)
    password = get_text_secret(PASSWORD_SECRET_OCID)
    logging.getLogger().info("password : " + password)
    db_url = get_text_secret(DB_URL_SECRET_OCID)
    logging.getLogger().info("db_url : " + db_url)
    write_db_wallet_files()
    #logging.getLogger().info('INFO: DB wallet dir content =' + os.listdir(DB_WALLET_PATH), flush=True)
    point_db_wallet_path()
    os.environ["TNS_ADMIN"] = DB_WALLET_PATH
    return cx_Oracle.connect(username, password, db_url)


def upload_posts(conn,lines):
    sql_query = ("INSERT INTO posts(post_media, post_hashtag, post_id, post_user, post_content, post_sentiment, post_date, created_at) values(:1,:2,:3,:4,:5,:6, to_timestamp(:7,'yyyy-mm-dd hh24.mi.ss.ff'),to_timestamp(:8,'yyyy-mm-dd hh24.mi.ss.ff'))")
    cursor = conn.cursor()
    cursor.executemany(sql_query, lines)
    conn.commit()



def get_text_sentiment(text):
    '''
    Utility function to classify sentiment of passed tweet
    using textblob's sentiment method
    '''
    # create TextBlob object of passed tweet text
    analysis = TextBlob(' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", text).split()))
    # set sentiment
    if analysis.sentiment.polarity > 0:
        return 'positive'
    elif analysis.sentiment.polarity == 0:
        return 'neutral'
    else:
        return 'negative'


def handler(ctx, data: io.BytesIO=None):
    logging.getLogger().info("function start")
    resp = ""
    try:
        con = connect_to_db()
        logging.getLogger().info("Successfully Connected DB...")

        body = json.loads(data.getvalue().decode('UTF-8'))
        hash_tag = body.get("hashtag")
        number = body.get("number")

        logging.getLogger().info("hashtag: " + hash_tag)
        logging.getLogger().info("number of twits: " + number)

        cur = con.cursor()
        cur.execute('SELECT COUNT(POST_MEDIA) FROM POSTS')
        rows = cur.fetchall()


        # Creating list to append tweet data to
        tweets_list = []
        post_media_type = 'twitter'
        start_date  = '2021-09-01'
        end_date    = '2022-02-28'
        query_str   = hash_tag + " since:" + start_date + " until:" + end_date

        for i,tweet in enumerate(sntwitter.TwitterSearchScraper(query_str).get_items()):
            if i>int(number):    #i>500:
                break
            sentiment = get_text_sentiment(text=tweet.content)
            # post_media, post_hashtag, post_id, post_user, post_content, post_sentiment, post_date, created_at
            #tweets_list.append([post_media_type, hash_tag, tweet.id, tweet.user.username, tweet.content, sentiment, tweet.date, datetime.datetime.now()])
            tweets_list.append([post_media_type, hash_tag, tweet.id, tweet.username, tweet.content, sentiment, tweet.date, datetime.datetime.now()])

        upload_posts(con, lines=tweets_list)

        cur = con.cursor()
        cur.execute('SELECT COUNT(POST_MEDIA) FROM POSTS')
        rows_after = cur.fetchall()

        resp = {"rows before": str(rows),
                "rows after": str(rows_after),
                "twits": str(tweets_list)}
    except Exception as e:
        print('ERROR: Missing configuration keys, secret ocid and secret_type', e, flush=True)
        logging.getLogger().info("function end")
    return response.Response(
        ctx,
        response_data=resp,
        headers={"Content-Type": "application/json"}
    )
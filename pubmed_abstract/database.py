import MySQLdb

from keys import HOST,USERNAME,PASSWORD,DATABASE

pubmed_insert = 'INSERT INTO posts(unique_id, \
           post_title, author, abstract,post_type, journal_name, doi, published_at) \
           VALUES ("%d", "%s", "%s", "%s","%s", "%s", "%s", "%s")'

filter_post_pivot_insert = "INSERT INTO filter_post_pivot(user_id, \
           filter_id, post_id) \
           VALUES ('%d', '%d', '%d')"

def get_database():
    """

    :return:
    """
    return MySQLdb.connect(HOST,USERNAME,PASSWORD,DATABASE)

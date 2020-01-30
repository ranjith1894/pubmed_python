# test
import json
import sys

from Bio import Entrez
from Bio import Medline
import MySQLdb
import threading
import datetime
from database import get_database, pubmed_insert,filter_post_pivot_insert

data_count = '350'

import time

_start_time = time.time()


def tic():
	global _start_time
	_start_time = time.time()


def tac():
	t_sec = round(time.time() - _start_time)
	(t_min, t_sec) = divmod(t_sec, 60)
	(t_hour, t_min) = divmod(t_min, 60)
	print('Time passed: {}hour:{}min:{}sec'.format(t_hour, t_min, t_sec))


def search(query):
	Entrez.email = 'your.email@example.com'
	handle = Entrez.esearch(db='pubmed',
							sort='relevance',
							retmax=data_count,
							retmode='text',
							term=query)
	results = Entrez.read(handle)

	return results


def fetch_details(id_list):
	"""
	:param id_list:
	:return:
	"""
	ids = ','.join(id_list)
	Entrez.email = 'your.email@example.com'
	handle = Entrez.efetch(db='pubmed',
						   retmode='text',
						   rettype="medline",
						   id=ids)
	# results = Entrez.read(handle)
	results = Medline.parse(handle)

	return results


if __name__ == '__main__':

	def fetch_data():
		s_arguments = sys.argv[1]
		db = get_database()
		cursor = db.cursor()
		json_args = json.loads(s_arguments)
		filter_id = json_args.get('filter_id')

		# start of Handle update and filter dislikes tags from pubmed
		optional_user_id = json_args.get('optional_user_id')
		temp_list = []
		# end of Handle update and filter dislikes tags from pubmed

		cursor.execute("SELECT term, user_id FROM filters WHERE id=%s" % (filter_id))
		filters_fields = cursor.fetchone()
		user_id = int(filters_fields[1])
		results = search(filters_fields[0])
		id_list = results['IdList']
		papers = fetch_details(id_list)
		print papers
		for r in papers:
			pmid = r['PMID']
			cursor.execute("SELECT id FROM posts WHERE unique_id=%s" % (int(pmid)))
			post = cursor.fetchone()
			if post:
				post_id = post[0]

				try:
					cursor.execute('SELECT id FROM filter_post_pivot WHERE filter_id="%s" AND user_id ="%s" AND post_id="%s"' % (filter_id, optional_user_id, post_id))
					fetch_pivot_id = cursor.fetchone()
					if fetch_pivot_id:
						temp_list.append(fetch_pivot_id[0])
					else :
						f_p_p_sql = filter_post_pivot_insert % \
								(user_id, int(filter_id), int(post_id))

						# Execute the SQL command
						cursor.execute(f_p_p_sql)
						# Commit your changes in the database
						db.commit()

						temp_list.append(cursor.lastrowid)

				except Exception as e:
					print("Error: ", e.message)
					# Rollback in case there is any error
					db.rollback()

			else:
				title = r['TI']
				abstraction = r.get('AB')
				author = r.get('AD')
				journal_name = r.get('JT')
				doi = r.get('LID')
				if doi:
					doi = doi.split()[-2]
				if abstraction:
					published_at = r['DP']
					sql = pubmed_insert % \
					  (int(pmid), '{0}'.format(title),'{0}'.format(author),'{0}'.format(abstraction),'3', '{0}'.format(journal_name), '{0}'.format(doi), '{0}'.format(published_at))
					cursor.execute(sql)
					db.commit()
					
					
					try:
						# Execute the SQL command
						cursor.execute(sql)
						# Commit your changes in the database
						db.commit()
						post_id = cursor.lastrowid

						other_tags = r.get('OT')
						if other_tags:
							for tag in other_tags:
								cursor.execute('SELECT id FROM tags WHERE tag_name="%s"' % (str(tag)))
								fetch_tag = cursor.fetchone()
								if fetch_tag:
									tag_id =fetch_tag[0]
								else:
									cursor.execute('INSERT INTO tags SET tag_name ="%s"' % (str(tag)))
									tag_id = cursor.lastrowid
								insert_query ="INSERT INTO post_tags(user_id, isOtherTag, post_id, tag_id) VALUES(%s,%s,%s,%s)"
								insert_tuple = (int(user_id), 1, int(post_id), int(tag_id))
								cursor.execute(insert_query,insert_tuple)

						try:
							f_p_p_sql = filter_post_pivot_insert % \
									(user_id, int(filter_id), int(post_id))

							# Execute the SQL command
							cursor.execute(f_p_p_sql)
							# Commit your changes in the database
							db.commit()

							temp_list.append(cursor.lastrowid)

						except Exception as e:
							print("Error: ", e.message)
							# Rollback in case there is any error
							db.rollback()
					except Exception as e:
						# Rollback in case there is any error
						db.rollback()

		# start of Handle update and filter dislikes tags from pubmed
		if optional_user_id and temp_list:

			format_strings = "(" + ",".join(map(str, temp_list)) + ")"
			cursor.execute('DELETE FROM filter_post_pivot WHERE filter_id="%s" AND user_id ="%s" AND id NOT IN %s' % (filter_id, optional_user_id, format_strings))
			db.commit()
		# end of Handle update and filter dislikes tags from pubmed

	threading.Thread(target=fetch_data, name ='fetch_data').start()
	print "Python Script Execution Started"
	sys.stdout.flush()

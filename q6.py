# Query 6
# Connectivity: Is the DBPL graph connected?

from cassandra.cluster import Cluster
cluster = Cluster()
session = cluster.connect('dblp')


def new_auth(ids_paper):
	list_of_authors = []
	for paper in range(len(ids_paper)):
		for each_paper in range(len(ids_paper[paper])):
			pid = ids_paper[paper][each_paper]
			author_per_paper = session.execute("SELECT authors from publications where publication_id = " + str(pid) + " ALLOW FILTERING")
	for m_auth in author_per_paper:
		for author_1 in range(len(m_auth.authors)):
			author_2 = m_auth.authors[author_1]
			list_of_authors.append(author_2)
		return list_of_authors

def set_pub(author1_id):
	pub_list1 = session.execute("SELECT publications FROM authors WHERE author_id = " +str(author1_id) + " ALLOW FILTERING")
	for paper in pub_list1:
		return paper
def conn_chk(authors_id):
	auth_trav.append(authors_id)
	paper = set_pub(authors_id)
	list_of_authors = new_auth(paper)
	list_of_authors = list(set(list_of_authors))
	for each_author in list_of_authors:
		if each_author in auth_trav:
			break
		else:
			conn_chk(each_author)
			return True

global auth_trav
auth_trav = []
authors = session.execute("SELECT author_id, publications FROM authors LIMIT 1 ALLOW FILTERING")
total_author_count = session.execute("SELECT count(*) as total_authors from authors")
tcount_auth = int(total_author_count[0][0])
for author in authors:
	author_id = author.author_id
	conn_chk(author_id)
if len(auth_trav) == tcount_auth:
	print('DBLP Graph is connected!')
else:
	print('DBLP Graph is not connected!')

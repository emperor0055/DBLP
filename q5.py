# Query 5
# Triangles: Which author participates in the most triangles?

from cassandra.cluster import Cluster
cluster = Cluster()
session = cluster.connect('dblp')

max_num = 0
max_author = []

author_ids = session.execute("SELECT author_id FROM authors ALLOW FILTERING")
for author_id in author_ids:
	count = 0
	initial_author = author_id.author_id
	publications_by_authors = session.execute("SELECT publications FROM authors WHERE author_id = "
                                + str(initial_author) + " ALLOW FILTERING")
	L1_Co_Authors = []
	L2_Co_Authors = []

        for rows in publications_by_authors:
		publications = rows.publications

        for publication in publications:
		authors_list = session.execute("SELECT authors FROM publications WHERE publication_id = " 
                             + str(publication) 
                             + " ALLOW FILTERING")

        for author in authors_list:
		for i in author.authors:
			if i != author: 
				L1_Co_Authors.append(i)

        L1_Co_Authors = list(set(L1_Co_Authors)) 
	coauthors1 = ''

	for i in L1_Co_Authors:
		coauthors1 = coauthors1 + str(i) + ',' 
	authors_list1 = coauthors1[:-1]

	publications_list = session.execute("SELECT publications FROM authors WHERE author_id IN ("
                          + str(authors_list1) 
                          + ") ALLOW FILTERING")

        for publications in publications_list:
		for row2 in publications: 
			publication_list = row2

        for level1_publications in publication_list:
		coauthors2 = session.execute("SELECT authors FROM publications WHERE publication_id = "
                           + str(level1_publications) 
                           + " ALLOW FILTERING")

	for author in coauthors2:
	    if level1_publications not in author.authors:
	        for i in author.authors:
		    L2_Co_Authors.append(i)

        L2_Co_Authors = list(set(L2_Co_Authors))
	L2_Co_Authors = list(set(L2_Co_Authors) - set(L1_Co_Authors))
        
        coauthors2 = ''
	for j in L2_Co_Authors:
		coauthors2 = coauthors2 + str(j) + ','
	authors_list2 = coauthors1[:-1]

	level2_publications = session.execute("SELECT publications FROM authors WHERE author_id IN (" 
                            + str(authors_list2) 
                            + ")ALLOW FILTERING")

	for publications in level2_publications:
		pub_list = ''
		for k in publications.publications:
			pub_list = pub_list + str(k) + ','
		pub_list = pub_list[:-1]

	coauthors3 = session.execute("SELECT authors FROM publications WHERE publication_id IN (" 
                   + str(pub_list) 
                   + ") ALLOW FILTERING")

	for i in coauthors3:
		if initial_author in i.authors and L1_Co_Authors not in i.authors:
			count += 1
			
	if max_num < count:
		del max_author[:]
		max_author.append(author)
		max_num = count
	else:
		if max_num == count:
			max_author.append(author)

print 'Authors with the most triangles:'
for author in max_author:
	author_details = session.execute("SELECT author_id, author_name FROM authors WHERE author_id = " 
                       + str(author) + " ALLOW FILTERING")
for row in author_details:
	print 'Author ID = ', row.author_id
	print 'Author Name = ', row.author_name

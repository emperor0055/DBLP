# Co-author count: Which publication has the most co-authors? 
# Give full information about the paper, including title, authors.

from cassandra.cluster import Cluster
cluster = Cluster()
session = cluster.connect('dblp')

# Fetch all publication Ids with their authors
publications = session.execute("SELECT publication_id, authors FROM publications ALLOW FILTERING")
max_num_authors = 0
publications_list = []

# Check author count for each publication,
# If the max_count is less than the number of authors, update the max_count
# and save the publication id
for publication in publications:
    if max_num_authors < len(publication.authors):
        del publications_list[:]
        publications_list.append(publication.publication_id)
        max_num_authors = len(publication.authors)
    else:
        if max_num_authors == len(publication.authors):
            publications_list.append(publication.publication_id)

# For each publication with the max_count, get the publication details
for publication in publications_list:
     publication_details = session.execute("SELECT * FROM publications WHERE publication_id = "
                         + str(publication) + " ALLOW FILTERING")
     print('Publication with the most coauthors:')
     for row in publication_details:
         print 'Publication ID: ', row.publication_id
         print 'Publication Title: ', row.publication_title
         print 'Publication Published As: ', row.publication_type + ' in:  ', row.published_in
         print 'Published in Year: ', row.publication_year
        
         authors_list = ''
         for i in row.authors:
             authors_list = authors_list + str(i) + ','
         authors_list = authors_list[:-1]                    
         author_details = session.execute("SELECT author_name FROM authors WHERE author_id IN (" 
                        + str(authors_list)
                        + ") ALLOW FILTERING")
         print 'Publication Authored by:'
         count =0
         for author in author_details:
             count = count + 1
             print(author.author_name)

print 'Total authors: ', count

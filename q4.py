# Query 4
# Most authors: Which proceedings in 2004 had the most distinct authors across all papers?

from cassandra.cluster import Cluster
cluster = Cluster()
session = cluster.connect('dblp')

authors_max_count = 0
result_proceedings = []
query_year = 2004

# Get all the proceedings in the query year
proceedings = session.execute("SELECT proceedings_id, authors FROM proceedings WHERE publication_year = "  + str(query_year) + " ALLOW FILTERING")

# For every proceeding in 2004 get the count of the authors
for proceeding in proceedings:
    if proceeding.authors is not None:
        # If the max_count is less than the number of authors, update the max_count
        # and save the proceeding id
        if authors_max_count < len(proceeding.authors):
            del result_proceedings[:]
            result_proceedings.append(proceeding.proceedings_id)
            authors_max_count = len(proceeding.authors)
        else:
            if authors_max_count == len(proceeding.authors):
                result_proceedings.append(proceedings.proceedings_id)

print '\nProceedings with the most distinct authors: \n'

# For each proceeding with the max_count, get the proceeding details
for proceeding in result_proceedings:
    Proceeding_Details = session.execute("SELECT proceedings_Id, publication_title FROM proceedings "
                       + "WHERE proceedings_id = " 
                       + str(proceeding) + " ALLOW FILTERING")
    for row in Proceeding_Details:
        print 'Proceedings ID: ', row.proceedings_id
        print 'Proceedings Title: ', row.publication_title

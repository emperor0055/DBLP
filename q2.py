# Query 2
# Recursive co-authors: How many Level 3 co-authors does Michael Stonebraker have?
# How many does David DeWitt have? (See below for definition.)


from cassandra.cluster import Cluster
cluster = Cluster()
session = cluster.connect('dblp')

# Initialize variables
author_name1="Michael Stonebraker"
author_name2="David J. DeWitt"

def count_level_3_co_authors(author_name):
    author_id = session.execute("SELECT author_id FROM authors WHERE author_name = '" + author_name+ "' ALLOW FILTERING")
    for rowdata in author_id:
        michael_author_id = rowdata.author_id

    # Get the publication ids authored by Michael Stonebraker 
    michael_publications = session.execute("SELECT publications FROM authors WHERE author_id = "
                        + str(michael_author_id) 
                        + " ALLOW FILTERING")

    # Initialize the variables
    L1_Authors = []
    L2_Authors = []
    L3_Authors = []

    # Level 1 Co-Authors
    for row in michael_publications:
        publications = row.publications
    # For each publication by David J. DeWitt, get all the co-authors and add to Level1 list
    for publication in publications:
        authors_list = session.execute("SELECT authors FROM publications WHERE publication_id = "
                    + str(publication) + " ALLOW FILTERING")

        for author in authors_list:
            for i in author.authors:
                if i != michael_author_id:
                    L1_Authors.append(i)
    L1_Authors = list(set(L1_Authors))
    authors_list = ''
    for i in L1_Authors:
        authors_list = authors_list + str(i) + ','
    authors_list = authors_list[:-1]
        
    # Level 2 Co-Authors
    # For each of the co-authors in Level1, get the publications by them and find their co-authors.
    publications_list = session.execute("SELECT publications FROM authors WHERE author_id IN ("
                    + str(authors_list) + ") ALLOW FILTERING")
    for publications in publications_list:
        for row in publications:
            publication_list = row
        for publication in publication_list:
            authors_list = session.execute("SELECT authors FROM publications WHERE publication_id = "
                        + str(publication) + " ALLOW FILTERING")
            for author in authors_list:
                for i in author.authors:
                    if i != michael_author_id:
                        L2_Authors.append(i)
    L2_Authors = list(set(L2_Authors))
    L2_Authors = list(set(L2_Authors) - set(L1_Authors))
    authors_list = ''
    for i in L2_Authors:
        authors_list = authors_list + str(i) + ','
    authors_list = authors_list[:-1]

    # Level 3 Co-Authors
    # For each of the co-authors in Level2, get the publications by them and find their co-authors.
    publications_list = session.execute("SELECT publications FROM authors WHERE author_id IN ("
                    + str(authors_list) + ") ALLOW FILTERING")
    for publications in publications_list:
        for row in publications:
            publication_list = row
        for publication in publication_list:
            authors_list = session.execute("SELECT authors FROM publications WHERE publication_id = "
                        + str(publication) + " ALLOW FILTERING")
        for author in authors_list:
            for i in author.authors:
                if i != michael_author_id:
                    L3_Authors.append(i)
    L3_Authors = list(set(L3_Authors))
    L3_Authors = list(set(L3_Authors) - set(L2_Authors))
    print author_name + " has " + str(len(L3_Authors))+ " Level 3 co-authors"

# Execute the above method for both authors in the question
count_level_3_co_authors(author_name1)
count_level_3_co_authors(author_name2)


# Query 3
# Co-author distance: At what level is Moshe Vardi from Michael J. Franklin?

from cassandra.cluster import Cluster
cluster = Cluster()
session = cluster.connect('dblp')


# Initialize variables
global checked_authors
checked_authors = []


# Get the authors for the publications
def list_authors(publications):
    list_of_authors = []
    
    # For each publication in the publications list, get the authors
    for publication in publications:
        author_per_paper = session.execute("SELECT authors from publications where publication_id ="
                         + str(publication) 
                         + " ALLOW FILTERING")
        for author_ids in author_per_paper:
            for author in author_ids.authors:
                list_of_authors.append(author)

    return list(set(list_of_authors))


# Get the publications for the authors
def list_publications(authors):
    author_publications = []
    # For each author in the authors list, get the list of publications
    for author in authors:
        publications_list = session.execute("SELECT publications FROM authors WHERE author_id ="
                          + str(author) 
                          + " ALLOW FILTERING")
        for pub in publications_list[0][0]:
            author_publications.append(pub)

    return list(set(author_publications))


# Get author level from Moshe Vardi
def get_author_level(author_id, counter):
    checked_authors.append(author_id)
    
    # Get the publications for the authors
    publications = list_publications(author_id)
    
    # Get the authors for the publications
    list_of_authors = list_authors(publications)
    list_of_authors = [updated_list_of_authors for  updated_list_of_authors in list_of_authors if updated_list_of_authors not in checked_authors]
    
    # If the authors list contains the id of Moshe Y. Vardi, exit
    # else find the next level of authors
    if moshe_vardi_author_id in list_of_authors:
        print "Moshe Y. Vardi and Michael J. Franklin are at co-author distance: " + str(counter)
        exit(0)
    else:
        counter = counter + 1
        author_id = []
        author_id.append(list(list_of_authors))

    return get_author_level(author_id[0], counter)


# main

# Initialize the variables
author_id = []

# Get the author_id of Moshe Y. Vardi
moshe_vardi_author_id = session.execute("SELECT author_id FROM authors WHERE author_name = 'Moshe Y. Vardi' ALLOW FILTERING")
moshe_vardi_author_id = int(moshe_vardi_author_id[0][0])

# Get the author_id of Michael J. Franklin
michael_franklin_author_id = session.execute("SELECT author_id FROM authors WHERE author_name = 'Michael J. Franklin' ALLOW FILTERING")
author_id.append(list(michael_franklin_author_id[0]))

# Find the author level
get_author_level(author_id[0], 1)

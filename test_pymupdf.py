import pymupdf

from openparse import processing, DocumentParser


from pipe import select, take
from itertools import count

from openai import OpenAI

import subprocess

def get_secret(secret):
    return(
        subprocess.run("pass show " + secret, shell = True,
                       stdout=subprocess.PIPE, text = True).stdout.strip())


import bibtexparser

FILE_BIB1 = "/


bib_database = with open(



# if __name__ == "__main__":

DIR_PROJ = "/home/johannes/Dropbox/proj/litanai/"
    

DIR_LIT = "/home/johannes/Dropbox/readings/"
test_doc = DIR_LIT + "Fasche_2013_history.pdf"



# pymupdf
doc = pymupdf.open(test_doc)

l_pages = []

for page in doc:
    text = page.get_text()
    l_pages.append(text)

len(l_pages)

doc_txt = "\n".join(l_pages)

with open(DIR_PROJ + "fasche_mupdf.txt", "w") as file:
    # Write text to the file
    file.write(doc_txt)



# openparse



parser = DocumentParser()
parsed_basic_doc = parser.parse(test_doc)

l_nodes_text = []

for node in parsed_basic_doc.nodes:
    l_nodes_text.append(node.text)
        
with open(DIR_PROJ + "fasche_openparse.txt", "w") as file:
    file.write("\n".join(l_nodes_text))



# display doesn't work in emacs
# pdf = openparse.Pdf(test_doc)

# pdf.display_with_bboxes(
#     parsed_basic_doc.nodes,
# )



# query testing
client = OpenAI(api_key = get_secret("openai-key"))

prompt = """you will read a long text. the text is in some way about private art museums, a new form of museums started by wealthy collectors. you have to find every instance in this text about how private art museums have an effect on the arts, for example that the artists after being exhibted experience a boost to their career, increase their chances of canonization or consecration, are more likely to be raise higher prices at auctions or are more likely to be exhibited by other museums or institutions. Any impact that private museums leave in the field of artistic production.

list every instance that you find literally word for word, don't rephrase anything. include all the text of each instance that is necessary for it to be understood when standing on its own. Do not use ellipses, include each instance from start to finish, even if inbetween there are elements that may seem less relevant. Rather include too much text than too little, it is important that the point of each instance is clearly understandable

if this text does not concern canonization, consecration or artist careers, just say so shortly

The next follows below this line:

"""

query = prompt + doc_txt


querry_res = client.chat.completions.create(
    messages = [
        {
            "role": "user",
            "content": query,
            }
        ],
    # model = "gpt-3.5-turbo",
    model = "gpt-4o-mini")


print(querry_res.to_dict()['choices'][0]['message']['content'])




#!/usr/bin/python

import re
import sys, getopt
import urllib2
import textwrap
from bs4 import BeautifulSoup as bs
from cStringIO import StringIO

#################################################################################
class DocWrapper(textwrap.TextWrapper):
    """Wrap text in a document, processing each paragraph individually"""

    def wrap(self, text):
        """Override textwrap.TextWrapper to process 'text' properly when
        multiple paragraphs present"""
        para_edge = re.compile(r"(\n\s*\n)", re.MULTILINE)
        paragraphs = para_edge.split(text)
        wrapped_lines = []
        for para in paragraphs:
            if para.isspace():
                if not self.replace_whitespace:
                    # Do not take the leading and trailing newlines since
                    # joining the list with newlines (as self.fill will do)
                    # will put them back in.
                    if self.expand_tabs:
                        para = para.expandtabs()
                    wrapped_lines.append(para[1:-1])
                else:
                    # self.fill will end up putting in the needed newline to
                    # space out the paragraphs
                    wrapped_lines.append('')
            else:
                wrapped_lines.extend(textwrap.TextWrapper.wrap(self, para))
        return wrapped_lines
# END class DocWrapper()

#################################################################################
def get_file_arg():
        filename = ''
        try:
            opts, args = getopt.getopt(sys.argv[1:], "f:", ["filename="])
        except getopt.GetoptError:
            print 'get_topics.py -f <filepath>'
            sys.exit(2)
        for opt, arg in opts:
            if opt == '-f':
               filename = arg
        return filename
# END get_file_arg()

#################################################################################
def parse_rel_note(html_text):
    wrapper = DocWrapper(subsequent_indent=' ' * 2)

    doc = bs(html_text, 'html.parser')

    # Find the release date
    for v in doc.find_all('p'):
        if 'Updated:' in v.text:
            rel_date = v.text.split(": ")[1]
            break

    TOPIC_LIST = ['product enhancement', 'hadoop distribution', 'supported platforms',
                   'new parameters', 'server configuration parameters',
                   'resolved issues', 'known issues',
                   'changed server', 'changed feature', 'deprecated feature']

    release_notes_all = StringIO()
    release_notes_by_topic = {}

    for topic in doc.find_all(['h2', 'h3', 'h4']):
        if topic.string and any (i in topic.string.lower() for i in TOPIC_LIST):
            parent = topic.find_parent('div')
            if parent:
                release_notes_all.write( '<>' * 35 + '\n' )
                tx = re.sub(' +', ' ', parent.get_text())
                formatted_text = wrapper.fill(tx)
                release_notes_all.write( formatted_text.encode('ascii', 'replace') )

                # Save a list of topics to store separately
                t1 = [x for x in TOPIC_LIST if x in topic.string.lower()]
                if t1:
                    release_notes_by_topic[t1[0]] = formatted_text.encode('ascii', 'replace')

    return release_notes_all.getvalue(), rel_date, release_notes_by_topic
# END parse_rel_note()

#################################################################################
if __name__ == "__main__":

    status = 0

    html_file = get_file_arg()
    html_text = ''
    try:
        with open (html_file, 'r') as f:
            html_text = f.read()
    except IOError as e:
        print "I/O error({0}): {1}".format(e.errno, e.strerror)
        status = 1
    except: #handle other exceptions such as attribute errors
        print "Unexpected error:", sys.exc_info()[0]
        status = 2

    if status == 0:
        notes_all, rel_date, notes_by_topic = parse_rel_note(html_text)
        print "Release Date: ", rel_date
        #print notes_all
        #print notes_by_topic
        for key in notes_by_topic:
            print '*' * 80
            print notes_by_topic[key]


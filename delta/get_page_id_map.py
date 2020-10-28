
# getmap resource_id repository_hostname csv_dest
#   writes a csv file

import sys, os, argparse

def getmap(id, repo, outfile):
  skip = 0
  limit = 100000
  more_to_do = True
  os.system('rm -f %s' % (outfile))
  os.system('touch %s' % (outfile))
  tail = ''
  while more_to_do:
    more_to_do = False
    size_before = os.path.getsize(outfile)
    os.system('wget -O - -q "%s/service/page_id_map/%s?skip=%s&limit=%s" %s >> %s' %\
              (repo, id, skip, limit, tail, outfile))
    size_after = os.path.getsize(outfile)
    sys.stderr.write("%s %s\n" % (skip, size_after))
    if (size_after - size_before) > 25:
      more_to_do = True
      skip += limit
      tail = '| tail -n +2'

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('id', help='resource id')
  parser.add_argument('dest', help='output file')
  parser.add_argument('repo', default='https://content.eol.org', nargs='?',
                      help='EOL content repository host name')
  args = parser.parse_args()
  getmap(args.id, args.repo, args.dest)

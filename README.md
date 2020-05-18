# 'Plotter' - make a graph

EOL related utilities

## Workspace structure

Workspace root comes from config.yml (via system.rb).  Currently set
to /home/jar/.plotter_workspace.

- (workspace root)/
   - dwca/
      - NNN/  ... one directory per DwCA ... id = lower 8 chars of uuid ?
         - properties.json
         - dwca.zip or dwca.tgz
         - unpacked/
            - meta.xml
            - (all the other tsv or csv files)
   - resources/    ... one resource-in-repository
      - TAG.PID.RID/  ... one per instance + publishing id + repository id
          - paint/     - temporary directory for intermediate files
          - (any other intermediate files: cached things, page id 
            map, reports, etc; none at present)
   - staging/  (parallels a directory tree on the staging server)
      - TAG.PID.RID/  ... one per instance + publishing id + repository id
         - vernaculars/vernaculars.csv
         - inferences/inferences.csv


An 'instance' is a (publishing, repository) server pair.
TAG is either 'prod' or 'beta' (never 'test').

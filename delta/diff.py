
import sys, os, csv

NO_MORE_PAGES = 1 << 62

def diff(path1, path2, outdir):
  if not os.path.isdir(outdir):
    os.makedirs(outdir)
  with open(path1, "r") as in1:
    reader1 = csv.reader(in1)
    header1 = next(reader1)
    with open(path2, "r") as in2:
      reader2 = csv.reader(in2)
      header2 = next(reader2)
      with open(os.path.join(outdir, "new.csv"), "w") as newish:
        newish_writer = csv.writer(newish)
        newish_writer.writerow(header2)
        with open(os.path.join(outdir, "delete.csv"), "w") as goneish:
          goneish_writer = csv.writer(goneish)
          goneish_writer.writerow(header1)
          with open(os.path.join(outdir, "change.csv"), "w") as changeish:
            changeish_writer = csv.writer(changeish)
            changeish_writer.writerow(["taxonID", "field", "from", "to"])
            row1 = None
            row2 = None
            in1_count = 0
            in2_count = 0
            gone_count = 0
            new_count = 0
            changed_count = 0
            unchanged_count = 0
            field_added_count = 0
            field_removed_count = 0
            losing_sci_count = 0
            slug_count = 0
            deleted_orphan_count = 0
            dup_count = 0
            previous2 = -1
            while True:
              if not row1:
                try:
                  row1 = next(reader1)
                  page1 = int(row1[0])
                  in1_count += 1
                  if (page1 % 422917) == 0:
                    print("page %s" % page1)
                except StopIteration:
                  row1 = None
                  page1 = NO_MORE_PAGES
              if not row2:
                try:
                  row2 = next(reader2)
                  page2 = int(row2[0])
                  in2_count += 1
                except StopIteration:
                  row2 = None
                  page2 = NO_MORE_PAGES
              if page1 < page2:
                # delete page1
                goneish_writer.writerow(row1)
                gone_count += 1
                if not any(row1[1:]):
                  slug_count += 1
                elif not row1[1]:
                  deleted_orphan_count += 1
                row1 = None         # force read in1
              elif page2 < page1:
                # insert page2
                newish_writer.writerow(row2)
                new_count += 1
                row2 = None         # force read in2
                previous2 = page2
              elif page1 == NO_MORE_PAGES:
                # both inputs are exhausted
                break
              else:
                # page persists but some fields might be different
                changed = False
                field_added = False
                field_removed = False
                losing_sci = False
                # Not so good, would be better to go by name
                for i in range(1, min(len(row1), len(row2))):
                  a = row1[i]
                  b = row2[i]
                  if a != b:
                    field = header1[i]
                    if (field == "scientificName"
                        and ((is_scientific(a)
                              and not is_scientific(b))
                             or a.startswith(b))):
                      if page1 % 13773 == 0:
                        print("Losing scientific: %s -> %s" % (a, b))
                      losing_sci = True
                    elif (field == "taxonomicStatus"
                          and a == "accepted"
                          and b == "valid"):
                      pass
                    elif b == "?":
                      # Don't replace useful information with ?
                      #  (but could parent pointer point to deleted node???)
                      pass
                    elif field == "nodeID":
                      pass
                    else:
                      changeish_writer.writerow([page1, field, a, b])
                      if a and b:
                        changed = True
                      elif a:
                        field_removed = True
                      elif b:
                        field_added = True
                # This isn't quite right.
                if changed:
                  changed_count += 1
                elif field_added:
                  field_added_count += 1
                elif field_removed:
                  field_removed_count += 1
                elif losing_sci:
                  losing_sci_count += 1
                else:
                  unchanged_count += 1
                row1 = None
                row2 = None
                previous2 = page2
            print("Input 1:       %8s" % in1_count, file=sys.stderr)
            print("Input 2:       %8s" % in2_count, file=sys.stderr)
            print("New:           %8s" % new_count, file=sys.stderr)
            print("Deleted:       %8s" % gone_count, file=sys.stderr)
            print("  trivial:     %8s" % slug_count, file=sys.stderr)
            print("  orphan:      %8s" % deleted_orphan_count, file=sys.stderr)
            print("  other:       %8s" % (gone_count -
                                          (slug_count + deleted_orphan_count)),
                  file=sys.stderr)
            print("Field changed: %8s" % changed_count, file=sys.stderr)
            print("Field added:   %8s" % field_added_count, file=sys.stderr)
            print("Field removed: %8s" % field_removed_count, file=sys.stderr)
            print("Bad science:   %8s" % losing_sci_count, file=sys.stderr)
            print("Unchanged:     %8s" % unchanged_count, file=sys.stderr)
            print("Duplicate:     %8s" % dup_count, file=sys.stderr)

def is_scientific(name):
  return (' 1' in name or \
          ' 2' in name)

if __name__ == '__main__':
  path1 = sys.argv[1]
  path2 = sys.argv[2]
  outdir = sys.argv[3]
  diff(path1, path2, outdir)


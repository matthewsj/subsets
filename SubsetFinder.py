from collections import defaultdict, deque
import heapq

def LargestSupersets(setlists):
  '''Computes, for each item in the input, the largest superset in the same input.

setlists: A list of lists, each of which represents a set of items. Items must be hashable.
  '''
  # First, build a table that maps each element in any input setlist to a list of records
  # of the form (-size of setlist, index of setlist), one for each setlist that contains
  # the corresponding element
  element_to_entries = defaultdict(list)
  for idx, setlist in enumerate(setlists):
    entry = (-len(setlist), idx)  # cheesy way to make an entry that sorts properly -- largest first
    for element in setlist:
      element_to_entries[element].append(entry)

  # Within each entry, sort so that larger items come first, with ties broken arbitrarily by
  # the set's index
  for entries in element_to_entries.values():
    entries.sort()

  # Now build up the output by going over each setlist and walking over the entries list for
  # each element in the setlist. Since the entries list for each element is sorted largest to
  # smallest, the first entry we find that is in every entry set we pulled will be the largest
  # element of the input that contains each item in this setlist. We are guaranteed to eventually
  # find such an element because, at the very least, the item we're iterating on itself is in
  # each entries list.
  output = []
  for idx, setlist in enumerate(setlists):
    num_elements = len(setlist)
    buckets = [element_to_entries[element] for element in setlist]

    # We implement the search for an item that appears in every list by maintaining a heap and
    # a queue. We have the invariants that:
    #   1. The queue contains the n smallest items across all the buckets, in order
    #   2. The heap contains the smallest item from each bucket that has not already passed through
    #        the queue.
    smallest_entries_heap = []
    smallest_entries_deque = deque([], num_elements)
    for bucket_idx, bucket in enumerate(buckets):
      smallest_entries_heap.append((bucket[0], bucket_idx, 0))
    heapq.heapify(smallest_entries_heap)

    while (len(smallest_entries_deque) < num_elements or
           smallest_entries_deque[0] != smallest_entries_deque[num_elements - 1]):
      # First extract the next smallest entry in the queue ...
      (smallest_entry, bucket_idx, element_within_bucket_idx) = heapq.heappop(smallest_entries_heap)
      smallest_entries_deque.append(smallest_entry)

      # ... then add the next-smallest item from the bucket that we just removed an element from
      if element_within_bucket_idx + 1 < len(buckets[bucket_idx]):
        new_element = buckets[bucket_idx][element_within_bucket_idx + 1]
        heapq.heappush(smallest_entries_heap, (new_element, bucket_idx, element_within_bucket_idx + 1))

    output.append((idx, smallest_entries_deque[0][1]))

  return output



import re

def get_closest_matching_item(item_to_match, list_of_items):
  """Returns the closest matching item from a list of items.

  Args:
    item_to_match: The item to match.
    list_of_items: A list of items.

  Returns:
    The closest matching item, or None if no matching item is found.
  """

  closest_matching_item = None
  lowest_levenshtein_distance = float('inf')

  for item in list_of_items:
    # Calculate the Levenshtein distance between the two items.
    distance = distance_levenshtein(item_to_match, item)

    if distance < lowest_levenshtein_distance:
      closest_matching_item = item
      lowest_levenshtein_distance = distance

  return closest_matching_item


def distance_levenshtein(s1, s2):
  """Calculates the Levenshtein distance between two strings.

  Args:
    s1: The first string.
    s2: The second string.

  Returns:
    The Levenshtein distance between the two strings.
  """

  table = [[0 for _ in range(len(s2) + 1)] for _ in range(len(s1) + 1)]

  # Initialize the table.
  for i in range(len(table)):
    for j in range(len(table[0])):
      if i == 0:
        table[i][j] = j
      elif j == 0:
        table[i][j] = i
      else:
        table[i][j] = min(table[i - 1][j] + 1, table[i][j - 1] + 1,
                           table[i - 1][j - 1] + (s1[i - 1] != s2[j - 1]))

  return table[len(s1)][len(s2)]
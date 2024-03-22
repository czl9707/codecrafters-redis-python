import random
import string


# helper functions
def get_random_replication_id():
    return "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in range(40)
    )

from dotenv import load_dotenv
from aphiasync.sqlite import match
import gnparser


load_dotenv()


names = ["Abra alba", "Abra alba (W. Wood 1802)", "Abra alba W. Wood 1802", "Abra (Abra) Lamarck, 1818", "Abra Lamarck, 1818", "Abra alva", "Larus dominicanus dominicanus", "Skeletonema menzellii", "Paridotea munda", "Paridotea munda Hale, 1924", "Paridotea munda Nunomura, 1988", "Ulva lactuca"]
match(names)

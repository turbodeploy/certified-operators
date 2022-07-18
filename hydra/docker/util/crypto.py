import base64
from Crypto.Cipher import AES

# Get the master secret from the specified filename and return it as bytes.
def get_master_secret(file_name):
    file_in = open(file_name, 'rb')
    master_secret_bytes = file_in.read(-1)
    return master_secret_bytes

# Encrypt the plaintext string that is passed in using AES.MODE_GCM with the 256 bit master key
# that is passed in as bytes. Return a base64 encoded string
def encrypt(plaintext, master_secret):
    cipher = AES.new(master_secret, AES.MODE_GCM)
    nonce = cipher.nonce
    encrypted_bytes = bytearray(nonce)
    cipher_text = cipher.encrypt(plaintext.encode())
    encrypted_bytes.extend(cipher_text)
    encrypted_bytes.extend(cipher.digest())
    return base64.b64encode(bytes(encrypted_bytes)).decode("utf-8")

# Take ciphertext as a base64 encoded string and return the plaintext string you get after decoding
# it with the passed in master key. Raises ValueError if the decoding fails. This can happen if the
# master key was rotated.
def decrypt(ciphertext, master_secret):
    ciphertext_bytes = base64.b64decode(ciphertext.encode())
    # First 16 bytes are nonce and last 16 are the tag. The part in the middle is the ciphertext.
    nonce = ciphertext_bytes[:16]
    stop = len(ciphertext_bytes) - 16
    encrypted_text = ciphertext_bytes[16:stop]
    tag = ciphertext_bytes[stop:]
    cipher = AES.new(master_secret, AES.MODE_GCM, nonce=nonce)
    plain_bytes = cipher.decrypt(encrypted_text)
    # If the tag is not correct, it means the master key we used was wrong, probably because the
    # key has been rotated. Calling code should get the old key and try to decrypt with that.
    try:
        cipher.verify(tag)
        return plain_bytes.decode('UTF-8')
    except ValueError as e:
        raise e


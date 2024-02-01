"""
1.https://docs.python.org/3.11/library/hashlib.html#
2.https://www.youtube.com/watch?v=i-h0CtKde6w - Hashing in Python"""

import hashlib

class GeradorHash:
    def __init__(self):
        """ acessando o modulo hashlib e escolhendo o algoritmo sha1 """
        self.hash_sha1 = hashlib.sha1()
        
    """ recebendo uma string, para processar o hash"""  
    def processando_hash(self, string):
        self.hash_sha1.update(self.to_bytes(string))
        """ utilizando a funcao yield para processar um valor, no caso o (texto) que será digitado pelo usuario e,
        em seguida será gerado o hash """
        yield self.hash_sha1.hexdigest()
        
    """ método responsável por converter uma string para bytes usando o encoding utf-8"""
    def to_bytes(self, string):
        return bytes(string, 'utf-8')

def main():
    hash_generator = GeradorHash()

    while True:
        string = input("Por favor, digite um texto: ")
        """ funcao next, para obter o valor produzido pelo yield dentro do método """
        hash_sha1 = next(hash_generator.processando_hash(string))
        print("Address:", hash_sha1)

if __name__ == "__main__":
    main()
from generator import GQLParser
p=GQLParser(verbose=True)
s=open('d:/GQL/Sample Queries/count.gql','r',encoding='utf-8').read()
print('TOKENS:')
for t in p.tokenize(s):
    print(t)
print('\nPARSED AST:')
print(p.parse(s))

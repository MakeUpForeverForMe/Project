import urllib.parse

chinese_str = '哈哈哈哈哈啊哈哈'
utf8_str_quote = urllib.parse.quote(chinese_str, encoding='utf-8')
print(utf8_str_quote)

unquote = urllib.parse.unquote(utf8_str_quote, encoding='utf-8')
print(unquote)

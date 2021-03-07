from random import randint


class Password(object):

    def __init__(self):
        self.nu = '0123456789'
        self.en = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
        self.sp = '!#$%&()*+,-./:;<=>?@[]^_{|}~'

    def password_digits(self, digits, char_number=True, char_english=True, char_special=True):
        if char_number == char_english == char_special is False:
            return

        character = ''
        char = ''

        if char_number:
            character += self.nu
        if char_english:
            character += self.en
        if char_special:
            character += self.sp

        for index in range(digits):
            char += character[randint(0, len(character) - 1)]

        return char


if __name__ == '__main__':
    password = Password()

    passwd = password.password_digits(8)

    print(passwd)

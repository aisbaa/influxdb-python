# -*- coding: utf-8 -*-

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest

from influxdb import line_protocol
from influxdb.line_protocol import _convert_timestamp, make_line


class TestLineProtocol(unittest.TestCase):

    def test_make_lines(self):
        data = {
            "tags": {
                "empty_tag": "",
                "none_tag": None,
                "integer_tag": 2,
                "string_tag": "hello"
            },
            "points": [
                {
                    "measurement": "test",
                    "fields": {
                        "string_val": "hello!",
                        "int_val": 1,
                        "float_val": 1.1,
                        "none_field": None,
                        "bool_val": True,
                    }
                }
            ]
        }

        self.assertEqual(
            line_protocol.make_lines(data),
            'test,integer_tag=2,string_tag=hello '
            'bool_val=True,float_val=1.1,int_val=1i,string_val="hello!"\n'
        )

    def test_string_val_newline(self):
        data = {
            "points": [
                {
                    "measurement": "m1",
                    "fields": {
                        "multi_line": "line1\nline1\nline3"
                    }
                }
            ]
        }

        self.assertEqual(
            line_protocol.make_lines(data),
            'm1 multi_line="line1\\nline1\\nline3"\n'
        )

    def test_make_lines_unicode(self):
        data = {
            "tags": {
                "unicode_tag": "\'Привет!\'"  # Hello! in Russian
            },
            "points": [
                {
                    "measurement": "test",
                    "fields": {
                        "unicode_val": "Привет!",  # Hello! in Russian
                    }
                }
            ]
        }

        self.assertEqual(
            line_protocol.make_lines(data),
            'test,unicode_tag=\'Привет!\' unicode_val="Привет!"\n'
        )

    def test_quote_ident(self):
        self.assertEqual(
            line_protocol.quote_ident(r"""\foo ' bar " Örf"""),
            r'''"\\foo ' bar \" Örf"'''
        )

    def test_quote_literal(self):
        self.assertEqual(
            line_protocol.quote_literal(r"""\foo ' bar " Örf"""),
            r"""'\\foo \' bar " Örf'"""
        )

    def test_if_space_is_escaped_in_tag_key_tag_value_and_field_key(self):
        line = make_line('escaping', tags={'a b': '1 2'}, fields={'c d': 5})
        self.assertEqual(line, 'escaping,a\ b=1\ 2 c\ d=5i')

    def test_if_comma_escaped_in_tag_key_tag_value_and_field_key(self):
        line = make_line('escaping', tags={'a,b': '1,2'}, fields={'c,d': 5})
        self.assertEqual(line, 'escaping,a\,b=1\,2 c\,d=5i')

    def test_if_equal_escaped_in_tag_key_tag_value_and_field_key(self):
        line = make_line('escaping', tags={'a=b': '1=2'}, fields={'c=d': 5})
        self.assertEqual(line, 'escaping,a\=b=1\=2 c\=d=5i')

    def test_if_backslash_not_be_escaped(self):
        line = make_line(
            'backslash_escaping',
            tags={
                'C:\\Program Files': 12
            },
            fields={
                'C:\\Documents and Files': 5
            })
        print(line)
        self.assertEqual(
            line,
            (
                r'backslash_escaping,C:\Program\ Files=12'
                r' C:\Documents\ and\ Files=5i'
            )
        )


class TestConvertTimestamp(unittest.TestCase):

    def test_if_raises_value_error_when_not_supported(self):
        with self.assertRaises(ValueError):
            _convert_timestamp(object())

    def test_if_returs_unmodified_integral_values(self):
        self.assertEqual(_convert_timestamp(5), 5)
        self.assertEqual(_convert_timestamp(-2), -2)

// Problem Statement

// Longest Palindromic Substring Write a program to find the longest palindromic substring in a given string without using any built-in substring or reverse functions. For example, for input "babad", the output should be "bab" or "aba". Instructions: Avoid using any string handling libraries. Implement a solution that checks all substrings manually.

// Program:
#include <stdio.h>
int getLen(char *s)
{
    int len = 0;
    while (s[len] != '\0')
    {
        len++;
    }
    return len;
}

int is_Palindrome(char *st, int s, int e)
{
    while (s < e)
    {
        if (st[s] != st[e])
        {
            return 0;
        }
        s++;
        e--;
    }
    return 1;
}

void func(char *str)
{
    int max_Len = 1;
    int start = 0;
    int len = getLen(str);

    for (int i = 0; i < len; i++)
    {
        for (int j = i; j < len; j++)
        {
            int flag = is_Palindrome(str, i, j);
            if (flag && (j - i + 1) > max_Len)
            {
                start = i;
                max_Len = j - i + 1;
            }
        }
    }

    printf("Output is:");
    for (int i = 0; i < max_Len; i++)
    {
        printf("%c", str[start + i]);
    }
    printf("\n");
}

int main()
{
    char string[100];
    printf("Enter string: ");
    scanf("%s", string);
    func(string);
    return 0;
}
// Testcases:
// 1)input:”babad”
//   Output:
//   Output is:bab
// 2)input:”cbbd”
//   Output:
//   Output is:bb
// 3)input:”aaaa”
//   Output:
//   Output is:aaaa

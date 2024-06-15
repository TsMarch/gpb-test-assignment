import datetime
import itertools
import os
import time

import pandas as pd


def task1(filepath: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = pd.read_csv(filepath, sep="|")
    df = df.drop(columns=["Unnamed: 0"])
    df_unique = df.drop_duplicates()
    df_same_id = (
        df.groupby("id").filter(lambda x: len(x) > 1).drop_duplicates(keep="first")
    )
    return df_same_id, df_unique


def task2(inp: list[set, set, set, set]) -> dict:
    answers_dct = {}
    m = inp
    # 1
    length = sum([len(i) for i in m])
    answers_dct["Пункт 1"] = length
    # 2
    total = sum(itertools.chain(*m))
    total = sum([i for i in m for i in i])
    answers_dct["Пункт 2"] = total
    # 3
    avg = total / length
    answers_dct["Пункт 3"] = avg
    # 4
    tup = tuple([i for i in m for i in i])
    answers_dct["Пункт 4"] = tup
    return answers_dct


def task3() -> list[dict]:
    a = [[i for i in range(1, 4)], [i for i in range(4, 7)]]
    return [{f"k{i}": i for i in x} for x in a]


def task4(path: str) -> list | str:
    if not path:
        return "Программа не выполнилась, укажите путь для удаления файлов"
    files = os.listdir(path)

    files = [os.path.join(path, file) for file in files if os.path.isfile(file)]
    print("Задание 4")
    print("Файлы в папке до удаления:", *files, sep="\n")

    days = 24 * 3600
    current_time = time.time()
    n = current_time - days * 30

    for file in files:
        file_location = os.path.join(os.getcwd(), file)
        file_time = os.stat(file_location).st_mtime
        match file_time < n:
            case True:
                print(
                    f"""Файл {file} удален, поскольку был создан {
                    datetime.datetime.fromtimestamp(file_time)
                    }"""
                )
                os.remove(file_location)

    return [file for file in files if os.path.isfile(file)]


def task5(inp_word: str) -> list:
    words_lst = []
    with open("words.txt", "r", encoding="utf-8") as file:
        for line in file:
            word = line.rstrip()
            step = 1
            if word == inp_word:
                continue
            while step < len(word):
                inp_word_slice = inp_word[-step:]
                word_slice = word[:step]
                match inp_word_slice == word_slice:
                    case True:
                        words_lst.append(f"{inp_word}{word[step:]}")
                        break
                    case False:
                        step += 1
    return words_lst


if __name__ == "__main__":
    df_1_task1, df_2_task2 = task1("f.csv")
    print("Задание 1 ", 2 * "\n", df_1_task1, 2 * "\n", df_2_task2)
    task_2 = task2([{11, 3, 5}, {2, 17, 87, 32}, {4, 44}, {24, 11, 9, 7, 8}])
    print("\n" "Задание 2")
    for i in task_2:
        print(i, task_2[i])
    print("\n" "Задание 3", task3(), sep="\n")
    print("\n" "Задание 4", task4(""), sep="\n")
    print("\n" "Задание 5", task5("стык"), sep="\n")

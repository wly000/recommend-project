import happybase


def main():
    import happybase
    pool = happybase.ConnectionPool(size=10, host='hadoop-master', port=9090)
    with pool.connection() as conn:
        # history_recall = conn.table('history_recall')
        # data = history_recall.cells('reco:his:{}'.format(5).encode(),
        #                      'channel:{}'.format(18).encode())
        # print(data)

        similar_table = conn.table('article_similar')
        similar_article = similar_table.row(str('116644').encode(),
                                            columns=[b'similar'])
        # 相似文章相似度排序过滤，召回不需要太大的数据， 百个，千
        _srt = sorted(similar_article.items(), key=lambda item: item[1], reverse=True)
        print(_srt)
        print([int(i[0].split(b':')[1]) for i in _srt][:10])


if __name__ == '__main__':
    main()


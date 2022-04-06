import pandas as pd


if __name__ == "__main__":
    N = 1000
    skip_count = 0
    line_counter = 0
    mallet = pd.DataFrame(columns=['subj','lang','text'])
    for df in pd.read_json("webis-gmane-19-part04.json",orient="records", lines=True, chunksize=2*N):
       
        columns = df.columns
        new_df = pd.DataFrame(columns=columns)
        odds = [2*i+1 for i in range(N)]
        evens = [2*i for i in range(N)]
        indexes = df.iloc[evens]['index'].reset_index(drop=True)
        data = df.iloc[odds][columns[1:]].reset_index(drop = True)
        new_df['index'] = indexes
        new_df[columns[1:]] = data
        #print(new_df.columns)
        for index, row in new_df.iterrows():
            line_counter += 1
            if line_counter % 1000 == 0:
                print("PROCESSING LINE", line_counter)
            try:
                #important header is 'subject'
                subject = row['headers']['subject']
                #print(subject)
                #important segments are 'paragraph' 'quotation'
                seg = pd.DataFrame(row['segments'])
                # *******************************************************************************
                # -------------------- ADD MORE SEGMENTS HERE (if needed) -----------------------
                # *******************************************************************************
                seg = seg.loc[(seg['label'] == 'paragraph') | (seg['label'] == 'quotation')]
                seg = seg[['label','begin','end']]
                lang = row['lang']
                body = row['text_plain']
                text = ""
                for index, row in seg.iterrows():
                    start = row['begin']
                    end = row['end']
                    text += body[start:end]
                #print("-------EXTRACTED-------\n",text)
                #print("-------UNABRIDGED-------\n",body)
                # SUBJECT, LANGUAGE, BODY TEXT
                toadd = pd.DataFrame({'subj':subject, 'lang':lang, 'text':text}, index = [0,1,2])
                
                mallet = pd.concat([mallet,toadd], ignore_index = True)
            except:
                skip_count += 1
                print("SKIPPED LINE. count=",skip_count)
                print("CURRENT LINE", line_counter)
        mallet.to_csv("mallet_data.csv", index=False)
        


        

        
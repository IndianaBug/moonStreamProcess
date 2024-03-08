class testerOption():

    def test_empty_dataframes(self):
        """
        """
        a = []
        for exchange in self.aggregator.axis.keys():
            puts = self.aggregator.axis[exchange].df_put
            calls = self.aggregator.axis[exchange].df_call
            for rp, rc in zip(puts.keys(), calls.keys()):
                s = puts[rp]
                ss = puts[rc]
                try:
                    if s.empty:
                        print(f"rp, calls, {exchange}, is empty")
                        a.append(s)
                    if ss.empty:
                        print(f"rc, puts, {exchange}, is empty")
                        a.append(s)
                except:
                    print(f"rp, calls, {exchange}, is NoneType")
                    a.append(s)
                    print(f"rp, calls, {exchange}, is NoneType")
                    a.append(ss)
        if len(a) == 0:
            print("No empty frames")

    def display_dataframes(self):
        a = []
        for exchange in self.aggregator.axis.keys():
            puts = self.aggregator.axis[exchange].df_put
            calls = self.aggregator.axis[exchange].df_call
            for rp, rc in zip(puts.keys(), calls.keys()):
                s = puts[rp]
                ss = puts[rc]
                try:
                    display(s)
                    time.sleep(2)
                    display(ss)
                except:
                    pass
            time.sleep(2)
            clear_output(wait=True)




class tester():

    def test_empty_dataframes(self):
        """
        """
        a = []
        for flow in self.spot_axis.keys():
            for instrument in self.spot_axis[flow].keys():
                if flow != "indicators":
                    if flow in ["books", "oifunding"]:
                        s = self.spot_axis[flow][instrument].snapshot
                    if flow in ["trades", "liquidations"]:
                        s = self.spot_axis[flow][instrument].snapshot_total
                    try:
                        if s.empty:
                            print(f"perpetual_axis, {flow}, {instrument}, is empty")
                            a.append(s)
                    except:
                        print(f"perpetual_axis, {flow}, {instrument}, is NoneType")
                        a.append(s)
                if flow == "indicators":
                    if not bool( self.spot_axis[flow][instrument].data):
                        print(f"spot_axis, {flow}, {instrument}, is empty")
                        a.append(s)

        for flow in self.perpetual_axis.keys():
            for instrument in self.perpetual_axis[flow].keys():
                if flow != "indicators":
                    if flow in ["books", "oifunding"]:
                        s = self.perpetual_axis[flow][instrument].snapshot
                    if flow in ["trades", "liquidations"]:
                        s = self.perpetual_axis[flow][instrument].snapshot_total
                    try:
                        if s.empty:
                            print(f"perpetual_axis, {flow}, {instrument}, is empty")
                            a.append(s)
                    except:
                        print(f"perpetual_axis, {flow}, {instrument}, is NoneType")
                        a.append(s)
                    
                if flow == "indicators":
                    if not bool( self.perpetual_axis[flow][instrument].data):
                        print(f"perpetual_axis, {flow}, {instrument}, is empty")
                        a.append(s)
        
        if len(a) == 0:
            print("No empty frames")

    def display_dataframes(self):
        # Observe snapshots for any discrepancy
        for flow in self.spot_axis.keys():
            for instrument in self.spot_axis[flow].keys():
                a = []
                if flow != "indicators":
                    if flow in ["books", "oifunding"]:
                        s = self.spot_axis[flow][instrument].snapshot
                        a.append(s)
                    if flow in ["trades", "liquidations"]:
                        s = self.spot_axis[flow][instrument].snapshot_total
                        a.append(s)
                    try:
                        display(s)
                    except:
                        pass
                if flow == "indicators":
                    s = self.spot_axis[flow][instrument].data
                    a.append(s)
                    if len(s) != 0:
                        print(s)
                time.sleep(2)
                clear_output(wait=True)

        for flow in self.perpetual_axis.keys():
            for instrument in self.perpetual_axis[flow].keys():
                if flow != "indicators":
                    if flow in ["books", "oifunding"]:
                        s = self.perpetual_axis[flow][instrument].snapshot
                    if flow in ["trades", "liquidations"]:
                        s = self.perpetual_axis[flow][instrument].snapshot_total
                    try:
                        display(s)
                    except:
                        pass
                if flow == "indicators":
                    s = self.perpetual_axis[flow][instrument].data
                    print(s)
                time.sleep(2)
                clear_output(wait=True)
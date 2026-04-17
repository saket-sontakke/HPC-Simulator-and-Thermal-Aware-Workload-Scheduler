"use client";

import React, { useState, useMemo } from 'react';
import { ThemeableNumberInput } from '../ui/ThemeableNumberInput';
import { SchedulingMode } from '../../lib/simulator/types';
import { Settings, Upload, Sun, Moon, Home, Info, Folder, X, CheckSquare } from 'lucide-react';

interface ConfigViewProps {
  theme: 'dark' | 'light';
  onToggleTheme: () => void;
  nodeCount: number | '';
  onNodeChange: (e: any) => void;
  onNodeBlur: () => void;
  ambientTemp: number | '';
  onTempChange: (e: any) => void;
  onTempBlur: () => void;
  mode: SchedulingMode;
  onModeChange: (mode: SchedulingMode) => void;
  isABTest: boolean;
  onABTestChange: (val: boolean) => void;
  isUploading: boolean;
  uploadStats: { current: number; total: number };
  jobCount: number;
  onFileUpload: (e: React.ChangeEvent<HTMLInputElement>) => void;
  onLoadSampleFiles: (paths: string[]) => void; 
  onLaunch: () => void;
  onGoHome: () => void;
}

// --- SAMPLE DATA MANIFEST ---
const SAMPLE_MANIFEST: Record<string, string[]> = {
  "bert-base-uncased": [
    "1181085738973-r7343737-n851693.csv", "1262594548610-r629115-n43543.csv", "1808531699064-r7343737-n911952.csv", "2003720312370-r1682297-n911952.csv", "2312801397194-r8333645-n851693.csv", "2676655762435-r8939293-n136082.csv", "2896243579938-r4858666-n830961.csv", "3054925459831-r8579942-n208530.csv", "4225549041985-r3041626-n851693.csv", "439033388226-r2998125-n208530.csv", "4610501415540-r3879907-n136082.csv", "4652979158115-r1485405-n43543.csv", "6067762498061-r4179716-n911952.csv", "6156279086493-r9102715-n830961.csv", "684781084440-r5715171-n136082.csv"
  ],
  "conv": [
    "12259060320474-r2825489-n139058.csv", "15179646833041-r1682297-n851693.csv", "27804537652393-r8333645-n685852.csv", "32249095466700-r3741709-n685852.csv", "37981585217450-r3879907-n208530.csv", "56445763544776-r4822976-n139058.csv", "5774828082734-r3741709-n685852.csv", "58476744174890-r9189566-n830961.csv", "62125685928952-r8333645-n685852.csv", "63937018334140-r4179716-n851693.csv", "66128261934918-r4179716-n386398.csv", "71369780233844-r7343737-n386398.csv", "741295796350-r4229531-n386398.csv", "75893069107267-r7217787-n851693.csv", "9002016628354-r4858666-n976057.csv"
  ],
  "dimenet": [
    "12240452667165-r1485405-n685852.csv", "138910797515-r3041626-n851693.csv", "25136828463581-r4858666-n976057.csv", "34227226372540-r3117156-n139058.csv", "36442778948476-r629115-n976057.csv", "37425783630275-r4822976-n139058.csv", "38136510743038-r9189566-n976057.csv", "42715219390883-r629115-n976057.csv", "45471558590649-r9192091-n851693.csv", "5568431958164-r3117156-n139058.csv", "628722349496-r9189566-n386398.csv", "7346849778808-r2825489-n139058.csv", "80252560487278-r4179716-n851693.csv", "81018946202369-r3879907-n139058.csv", "9761351256032-r9192091-n386398.csv"
  ],
  "distilbert-base-uncased": [
    "1724744543993-r5130449-n139058.csv", "2174683918384-r4822976-n139058.csv", "2304959634945-r5715171-n136082.csv", "2608125844745-r5130449-n139058.csv", "2795191193018-r1485405-n976057.csv", "3505531003668-r629115-n830961.csv", "4912076150422-r629115-n830961.csv", "5422635441557-r3741709-n830961.csv", "5441612378702-r4179716-n911952.csv", "5452605660748-r9192091-n43543.csv", "5564312519548-r4229531-n911952.csv", "6233293023811-r3879907-n136082.csv", "6922608436725-r4822976-n139058.csv", "7157982830167-r8333645-n830961.csv", "7456187102621-r4179716-n911952.csv"
  ],
  "inception3": [
    "123768618927-r9352821-n830961.csv", "1912527379579-r8607415-n976057.csv", "2777540189652-r3226521-n976057.csv", "3055196760815-r9192091-n911952.csv", "3063043595250-r8579942-n136082.csv", "369972989432-r3117156-n139058.csv", "382824463538-r2100214-n976057.csv", "3945443032356-r1682297-n685852.csv", "4040740733045-r1682297-n43543.csv", "4283980613027-r1457839-n911952.csv", "4391237494359-r8937440-n43543.csv", "5319306603747-r2100214-n976057.csv", "5767154951333-r3741709-n830961.csv", "6471803447287-r4822976-n139058.csv", "7252588961361-r9192091-n685852.csv"
  ],
  "inception4": [
    "1173416277102-r8937440-n830961.csv", "2063739185347-r9102715-n43543.csv", "2222822427142-r629115-n911952.csv", "2759986725115-r4858666-n685852.csv", "3124731017640-r9192091-n911952.csv", "3412131683628-r8333645-n911952.csv", "3419101415399-r3741709-n685852.csv", "4479761871984-r9535192-n911952.csv", "531177744168-r629115-n976057.csv", "5650105580220-r7343737-n976057.csv", "5892990661811-r8062914-n139058.csv", "6109602899976-r4822976-n136082.csv", "611401690860-r9192091-n386398.csv", "70053314562-r9352821-n43543.csv", "772032503727-r6272977-n911952.csv"
  ],
  "pna": [
    "13037928135388-r3117156-n139058.csv", "14307000132792-r8607415-n911952.csv", "17296238620342-r3879907-n139058.csv", "20169988895085-r6760045-n976057.csv", "20276904406848-r3879907-n139058.csv", "21201596330943-r2652301-n911952.csv", "25298935991038-r3405251-n139058.csv", "36182023669935-r4822976-n139058.csv", "36230628475012-r9555635-n139058.csv", "41324135643800-r4858666-n911952.csv", "56873208958447-r2825489-n139058.csv", "60929646260754-r3117156-n139058.csv", "68382429293939-r4179716-n911952.csv", "75592954051300-r7343737-n911952.csv", "86021376763870-r8015356-n911952.csv"
  ],
  "resnet101": [
    "12997731412859-r9192091-n911952.csv", "1362354961121-r9102715-n911952.csv", "15682302863352-r3405251-n136082.csv", "1724085272716-r2998125-n208530.csv", "17833011698363-r7217787-n851693.csv", "24357584411588-r8333645-n685852.csv", "3611766027875-r3117156-n136082.csv", "3638154732417-r4327055-n208530.csv", "4315008632242-r2582019-n851693.csv", "5964332157888-r9555635-n208530.csv", "6689509094647-r1485405-n830961.csv", "6746962162754-r8642123-n911952.csv", "7199748004345-r7343737-n685852.csv", "8899355680331-r9189566-n851693.csv", "9668868281807-r1682297-n851693.csv"
  ],
  "resnet101_v2": [
    "11977056077463-r3741709-n685852.csv", "15631132814357-r9192091-n851693.csv", "21190700544403-r4858666-n685852.csv", "24996312007178-r8333645-n685852.csv", "26485316306177-r5715171-n136082.csv", "26870334205256-r5130449-n136082.csv", "27097492684727-r8579942-n139058.csv", "29257714836313-r9192091-n685852.csv", "31247799420145-r2652301-n851693.csv", "31360445131361-r4179716-n976057.csv", "31651718228103-r1457839-n911952.csv", "36591904113286-r9352821-n830961.csv", "40282934175382-r8062914-n136082.csv", "7192542193763-r3405251-n208530.csv", "7806101475212-r5130449-n208530.csv"
  ],
  "resnet152": [
    "1235623688522-r8937440-n911952.csv", "13911439659364-r8333645-n851693.csv", "15305255756787-r6760045-n685852.csv", "18593553555929-r8333645-n685852.csv", "20571202675986-r9102715-n851693.csv", "22184187876089-r9192091-n685852.csv", "25276759783697-r4179716-n911952.csv", "26185465701982-r8333645-n685852.csv", "30459674923938-r2825489-n208530.csv", "30540572226412-r9352821-n43543.csv", "32871034152128-r1682297-n685852.csv", "6463541871509-r8579942-n139058.csv", "6923956011792-r5573787-n911952.csv", "7842792242966-r5715171-n136082.csv", "8966876280759-r1485405-n851693.csv"
  ],
  "resnet152_v2": [
    "14577701923877-r9040233-n911952.csv", "21045770204504-r9189566-n830961.csv", "21330178464393-r9192091-n685852.csv", "23686420064956-r8937440-n830961.csv", "24649967882275-r5130449-n139058.csv", "25479629446849-r3741709-n976057.csv", "26050135217835-r5189505-n43543.csv", "27189927419097-r3741709-n685852.csv", "27214779287342-r3475376-n208530.csv", "30451420095529-r5715171-n208530.csv", "31421236388302-r8937440-n685852.csv", "32548140084588-r3117156-n136082.csv", "7195398171075-r6272977-n851693.csv", "7199619598553-r4858666-n830961.csv", "9481065906349-r8642123-n911952.csv"
  ],
  "resnet50": [
    "10201892587718-r7217787-n830961.csv", "10441214642239-r7217787-n830961.csv", "12377038147170-r8937440-n386398.csv", "13195655198409-r3226521-n830961.csv", "1565287780989-r4327055-n139058.csv", "1837213663243-r1682297-n43543.csv", "20924628006079-r9189566-n830961.csv", "20964010323856-r9192091-n830961.csv", "2457128028544-r1682297-n685852.csv", "2485245022538-r1485405-n830961.csv", "5990203172638-r3405251-n136082.csv", "6034432506353-r7217787-n830961.csv", "6659873823597-r4327055-n139058.csv", "7117156657691-r629115-n911952.csv", "8835225905106-r5715171-n139058.csv"
  ],
  "resnet50_v1.5": [
    "12698252974861-r5130449-n208530.csv", "13868654872730-r7217787-n830961.csv", "14584398470156-r9192091-n830961.csv", "14841306951735-r6272977-n911952.csv", "15131391309478-r5130449-n136082.csv", "15466830247540-r4229531-n976057.csv", "1556113608070-r3879907-n208530.csv", "16417687838186-r5130449-n139058.csv", "16518135870399-r1682297-n43543.csv", "17017070303336-r7217787-n830961.csv", "18342703426938-r3117156-n136082.csv", "355993399255-r8579942-n136082.csv", "9335386890281-r3041626-n976057.csv", "9618968776386-r7217787-n851693.csv", "9734628547732-r7217787-n830961.csv"
  ],
  "schnet": [
    "13138053704028-r9040233-n976057.csv", "1454614744097-r4858666-n830961.csv", "17063430544464-r9555635-n136082.csv", "18203079158032-r4327055-n136082.csv", "19335571136746-r1682297-n976057.csv", "23778495814751-r6272977-n851693.csv", "25696168767828-r8939293-n136082.csv", "28007586953857-r3405251-n139058.csv", "39511809026013-r3226521-n976057.csv", "41285841957506-r7343737-n43543.csv", "44366883012443-r9352821-n386398.csv", "44736509525151-r6760045-n851693.csv", "45271786510094-r8062914-n136082.csv", "8227771720637-r629115-n976057.csv", "9950242205838-r7217787-n851693.csv"
  ],
  "U3-128": [
    "1017917913525-r8333645-n685852.csv", "1059916241808-r9352821-n851693.csv", "10715038114699-r8937440-n976057.csv", "11984503488669-r8939293-n208530.csv", "3717817204413-r7217787-n851693.csv", "4417667443341-r6760045-n830961.csv", "4928792631376-r9535192-n851693.csv", "552969155977-r9040233-n851693.csv", "6193463265918-r4858666-n685852.csv", "6903484959743-r8607415-n911952.csv", "7372259421889-r1682297-n685852.csv", "8370909980721-r9352821-n685852.csv", "9230450558194-r5189505-n830961.csv", "9862076549920-r5573787-n386398.csv", "9949843714541-r8579942-n208530.csv"
  ],
  "U3-32": [
    "1647229951118-r8939293-n139058.csv", "1734501085422-r4858666-n911952.csv", "25417044304-r1485405-n976057.csv", "2958233925161-r7343737-n43543.csv", "3285417367264-r629115-n976057.csv", "4472419499583-r9102715-n911952.csv", "4894304181742-r1485405-n43543.csv", "5711808548001-r8062914-n208530.csv", "6486049140011-r7217787-n830961.csv", "6637612575124-r7343737-n43543.csv", "7006916431177-r6760045-n830961.csv", "7440702596508-r2652301-n43543.csv", "7812080909917-r3741709-n685852.csv", "7898577696209-r3741709-n386398.csv", "8696536280880-r5189505-n830961.csv"
  ],
  "U3-64": [
    "2110684905527-r9192091-n685852.csv", "2291755177745-r1485405-n911952.csv", "2381065838824-r4858666-n685852.csv", "2906804623607-r7343737-n976057.csv", "2970203459610-r9352821-n830961.csv", "4399319501092-r1485405-n685852.csv", "491748096454-r1485405-n911952.csv", "5058263743230-r9175025-n976057.csv", "5711101113966-r8579942-n208530.csv", "5787058459105-r9192091-n685852.csv", "5835698954499-r8333645-n830961.csv", "6181090853414-r1485405-n911952.csv", "8766940065340-r4858666-n685852.csv", "9731541575132-r9192091-n685852.csv", "9995706021250-r5189505-n830961.csv"
  ],
  "U4-128": [
    "11197143027102-r1485405-n685852.csv", "11228240564913-r2652301-n830961.csv", "11773730454134-r8937440-n911952.csv", "12112452958882-r8333645-n685852.csv", "12855806608496-r3475376-n208530.csv", "13199304485349-r7217787-n851693.csv", "13764896307959-r3475376-n208530.csv", "2226671450710-r1485405-n911952.csv", "3310623936406-r7343737-n685852.csv", "3310909328426-r9192091-n386398.csv", "5025624390747-r7343737-n976057.csv", "7610067500699-r5189505-n685852.csv", "9102650142908-r9352821-n830961.csv", "9109535923550-r8333645-n685852.csv", "9431185474940-r1485405-n685852.csv"
  ],
  "U4-32": [
    "13123306395204-r9535192-n911952.csv", "13144539955235-r3226521-n911952.csv", "14149502358788-r3741709-n685852.csv", "3329214498079-r9352821-n685852.csv", "3464329406478-r9555635-n208530.csv", "4074022319779-r8937440-n43543.csv", "4824708413382-r6760045-n830961.csv", "5010709763800-r8579942-n208530.csv", "5271947854910-r5189505-n43543.csv", "6011837356928-r8642123-n911952.csv", "6106144267054-r7217787-n830961.csv", "6271028336072-r6760045-n830961.csv", "6853873683185-r5573787-n851693.csv", "8745312735322-r6760045-n685852.csv", "9808120498795-r9352821-n851693.csv"
  ],
  "U4-64": [
    "10196963725707-r5715171-n139058.csv", "10538242258546-r2998125-n208530.csv", "12657143383484-r9352821-n43543.csv", "13623128684768-r9352821-n685852.csv", "13724083248085-r8937440-n43543.csv", "14147153356598-r7217787-n911952.csv", "2570561968793-r9352821-n43543.csv", "3334094986297-r8333645-n685852.csv", "501935063082-r3475376-n208530.csv", "6688897006318-r4327055-n136082.csv", "7791766478146-r2998125-n139058.csv", "8233123318472-r4229531-n386398.csv", "8919794515828-r1485405-n685852.csv", "92474349531-r9189566-n43543.csv", "9260505437560-r8579942-n208530.csv"
  ],
  "U5-128": [
    "10345538774753-r8333645-n386398.csv", "11226147811942-r7343737-n685852.csv", "14667730299298-r5715171-n208530.csv", "14927576277879-r2652301-n911952.csv", "15155562477860-r4858666-n43543.csv", "16179246329714-r4327055-n208530.csv", "16281852572090-r9555635-n208530.csv", "18365616116108-r9189566-n685852.csv", "18403433616093-r8642123-n911952.csv", "3128501754569-r8333645-n685852.csv", "7592448886337-r1682297-n976057.csv", "7671071272045-r4858666-n43543.csv", "7821418920768-r8333645-n830961.csv", "8062646133916-r4858666-n685852.csv", "9219359320939-r9102715-n830961.csv"
  ],
  "U5-32": [
    "1072692854834-r6760045-n685852.csv", "11023187361897-r9720335-n851693.csv", "1223889979597-r3879907-n208530.csv", "13296614245176-r3741709-n685852.csv", "14441333271082-r2998125-n208530.csv", "14995926905738-r5573787-n851693.csv", "1742261050470-r3741709-n685852.csv", "5198496692708-r4327055-n208530.csv", "5742651158092-r9535192-n976057.csv", "6253194729573-r9555635-n208530.csv", "6459593420670-r4327055-n208530.csv", "7006238511037-r9175025-n851693.csv", "8677668457825-r9102715-n830961.csv", "8733431909900-r8607415-n911952.csv", "9114273559837-r5189505-n976057.csv"
  ],
  "U5-64": [
    "10928220910059-r7343737-n685852.csv", "11004466459647-r629115-n976057.csv", "11024174384489-r9040233-n976057.csv", "12061204333884-r8062914-n139058.csv", "2645164483822-r2652301-n43543.csv", "3055102399362-r7343737-n685852.csv", "3167986040183-r8579942-n208530.csv", "3713838617011-r5573787-n386398.csv", "3902986109559-r8333645-n830961.csv", "3940427352263-r1485405-n685852.csv", "668170500007-r8642123-n851693.csv", "680974554190-r8579942-n208530.csv", "6817665544052-r9555635-n139058.csv", "8188578611349-r1485405-n43543.csv", "866728767288-r9352821-n976057.csv"
  ],
  "vgg11": [
    "2235225778786-r2100214-n976057.csv", "2285667452785-r1682297-n685852.csv", "2377887165227-r1457839-n976057.csv", "2462757021914-r1457839-n911952.csv", "2728109312077-r4858666-n685852.csv", "3083120564779-r6760045-n830961.csv", "330074354139-r4822976-n136082.csv", "3739810641722-r1682297-n685852.csv", "4210352044518-r4858666-n685852.csv", "4437245492349-r5130449-n139058.csv", "5086771685049-r9555635-n139058.csv", "5919416952372-r629115-n43543.csv", "6095421104287-r1485405-n830961.csv", "6611926988200-r8333645-n830961.csv", "7023555054233-r1682297-n685852.csv"
  ],
  "vgg16": [
    "10593510186555-r1682297-n685852.csv", "10927680614247-r3041626-n386398.csv", "11694990385721-r9555635-n139058.csv", "12169287774796-r8586363-n172998.csv", "1397301481090-r9175025-n911952.csv", "1747002524985-r1682297-n685852.csv", "2523128745265-r1682297-n43543.csv", "2729421787226-r7217787-n830961.csv", "3703503342840-r2652301-n851693.csv", "4400982438649-r7217787-n830961.csv", "4553620231381-r6760045-n830961.csv", "7694778786909-r3117156-n208530.csv", "8635639979205-r9352821-n386398.csv", "8944509058523-r8937440-n976057.csv", "9876362506229-r9192091-n386398.csv"
  ],
  "vgg19": [
    "1250055940134-r2652301-n976057.csv", "1301765423032-r8937440-n685852.csv", "1647518644488-r9189566-n685852.csv", "165239464944-r9555635-n139058.csv", "1662952430496-r6760045-n685852.csv", "1864191813774-r7343737-n685852.csv", "2358206523531-r3226521-n851693.csv", "4347886052085-r7343737-n976057.csv", "5213728886462-r9352821-n830961.csv", "5225407051109-r2825489-n139058.csv", "6539000352523-r1682297-n911952.csv", "8039664821470-r9192091-n386398.csv", "8820082055239-r3226521-n976057.csv", "9139205572787-r5573787-n976057.csv", "9744495343492-r3226521-n685852.csv"
  ]
};

export default function ConfigView(props: ConfigViewProps) {
  const percent = props.uploadStats.total > 0 
    ? Math.round((props.uploadStats.current / props.uploadStats.total) * 100) 
    : 0;

  // --- Modal State ---
  const [isSampleModalOpen, setIsSampleModalOpen] = useState(false);
  const [randomCount, setRandomCount] = useState<number | ''>(100);
  const [selectedCategory, setSelectedCategory] = useState<string>(Object.keys(SAMPLE_MANIFEST)[0]);
  const [selectedFiles, setSelectedFiles] = useState<Set<string>>(new Set());

  // Calculates 390
  const totalSampleJobs = useMemo(() => Object.values(SAMPLE_MANIFEST).flat().length, []);

  // --- Handlers ---
  const handleRandomize = () => {
    if (!randomCount || randomCount <= 0) return;
    const allPaths = Object.entries(SAMPLE_MANIFEST).flatMap(([category, files]) => 
      files.map(filename => `/samples/${category}/${filename}`)
    );
    const shuffled = [...allPaths].sort(() => 0.5 - Math.random());
    const selected = shuffled.slice(0, Math.min(randomCount, allPaths.length));
    
    props.onLoadSampleFiles(selected);
    setIsSampleModalOpen(false); 
  };

  const handleLoadManual = () => {
    const paths = Array.from(selectedFiles).map(file => `/samples/${selectedCategory}/${file}`);
    props.onLoadSampleFiles(paths);
    setSelectedFiles(new Set()); 
    setIsSampleModalOpen(false); 
  };

  const toggleFile = (filename: string) => {
    const next = new Set(selectedFiles);
    if (next.has(filename)) next.delete(filename);
    else next.add(filename);
    setSelectedFiles(next);
  };

  const handleSelectAll = () => {
    const currentFiles = SAMPLE_MANIFEST[selectedCategory];
    if (selectedFiles.size === currentFiles.length) {
      setSelectedFiles(new Set());
    } else {
      setSelectedFiles(new Set(currentFiles));
    }
  };

  return (
    <div className="flex-1 flex flex-col items-center justify-center p-2 sm:p-4 relative">
      
      {/* Custom Full-Screen Overlay Modal for Sample Jobs */}
      {isSampleModalOpen && (
        <div className="fixed inset-0 z-[100] flex items-center justify-center p-4 sm:p-8 bg-gray-900/60 dark:bg-black/80 backdrop-blur-sm animate-in fade-in duration-200">
          <div className="bg-white dark:bg-slate-900 w-full max-w-5xl rounded-2xl shadow-2xl flex flex-col max-h-full border border-gray-200 dark:border-slate-800 overflow-hidden animate-in zoom-in-95 duration-200">
            
            {/* Header */}
            <div className="flex justify-between items-center p-4 sm:p-5 border-b border-gray-100 dark:border-slate-800 bg-gray-50 dark:bg-slate-900/50 shrink-0">
              <h2 className="text-lg sm:text-xl font-bold text-gray-800 dark:text-gray-100">Load Sample Job Traces</h2>
              <button onClick={() => setIsSampleModalOpen(false)} className="p-2 text-gray-500 hover:bg-gray-200 dark:hover:bg-slate-700 rounded-lg transition-colors" title="Close">
                <X className="w-5 h-5" />
              </button>
            </div>
            
            {/* Body - Two Columns on Desktop */}
            <div className="flex-1 overflow-y-auto p-4 sm:p-6 flex flex-col md:flex-row gap-6 bg-gray-50/50 dark:bg-slate-950/50 min-h-0 custom-scrollbar">
              
              {/* Left Column: Tools */}
              <div className="w-full md:w-1/3 flex flex-col gap-6 shrink-0">
                
                {/* Randomizer Card */}
                <div className="bg-white dark:bg-slate-800 p-4 sm:p-5 rounded-xl border border-gray-200 dark:border-slate-700 shadow-sm flex flex-col gap-3">
                   <div className="flex justify-between items-center mb-1">
                      <h3 className="font-bold text-blue-600 dark:text-blue-400 text-sm">Auto-Randomizer</h3>
                      <span className="text-[11px] sm:text-xs text-blue-800 dark:text-blue-300 font-medium bg-blue-100 dark:bg-blue-900/50 px-2 py-1 rounded">{totalSampleJobs} Available</span>
                   </div>
                   <div className="space-y-1">
                     <label className="text-xs text-gray-500 dark:text-slate-400">Number of random jobs:</label>
                     <ThemeableNumberInput 
                       value={randomCount} 
                       onChange={(e) => setRandomCount(e.target.value === '' ? '' : parseInt(e.target.value))} 
                       onBlur={() => {!randomCount && setRandomCount(100)}} 
                       min={1} max={totalSampleJobs} 
                     />
                   </div>
                   <button onClick={handleRandomize} className="w-full bg-blue-600 hover:bg-blue-700 text-white mt-1 px-4 py-2.5 rounded-lg text-sm font-bold transition-colors shadow-sm">
                     Queue Random Jobs
                   </button>
                </div>

                {/* Directory Selector Card */}
                <div className="bg-white dark:bg-slate-800 p-4 sm:p-5 rounded-xl border border-gray-200 dark:border-slate-700 shadow-sm flex flex-col gap-3 flex-1">
                   <h3 className="font-bold text-gray-800 dark:text-gray-200 text-sm flex items-center gap-2 mb-1">
                     <Folder className="w-4 h-4 text-gray-500"/> Browse Directory
                   </h3>
                   <label className="text-xs text-gray-500 dark:text-slate-400">Select a model category:</label>
                   <select 
                     value={selectedCategory} 
                     onChange={(e) => { setSelectedCategory(e.target.value); setSelectedFiles(new Set()); }}
                     className="w-full bg-gray-50 dark:bg-slate-900 border border-gray-300 dark:border-slate-600 rounded-lg px-3 py-2.5 text-sm text-gray-700 dark:text-slate-200 outline-none focus:ring-2 focus:ring-blue-500 transition-shadow"
                   >
                     {Object.keys(SAMPLE_MANIFEST).map(cat => <option key={cat} value={cat}>{cat} ({SAMPLE_MANIFEST[cat].length} traces)</option>)}
                   </select>
                </div>
              </div>

              {/* Right Column: File List */}
              <div className="w-full md:w-2/3 bg-white dark:bg-slate-800 border border-gray-200 dark:border-slate-700 rounded-xl shadow-sm flex flex-col min-h-[300px] md:min-h-0 overflow-hidden">
                 
                 <div className="p-3 sm:p-4 border-b border-gray-100 dark:border-slate-700 bg-gray-50 dark:bg-slate-900/50 flex justify-between items-center shrink-0">
                   <span className="text-sm font-bold text-gray-800 dark:text-gray-200 font-mono">{selectedCategory}/</span>
                   <button onClick={handleSelectAll} className="text-xs sm:text-sm font-medium text-blue-600 dark:text-blue-400 hover:text-blue-700 dark:hover:text-blue-300 flex items-center gap-1 transition-colors">
                     <CheckSquare className="w-3.5 h-3.5" /> 
                     {selectedFiles.size === SAMPLE_MANIFEST[selectedCategory].length ? 'Deselect All' : 'Select All'}
                   </button>
                 </div>
                 
                 <div className="flex-1 overflow-y-auto p-2 sm:p-3 custom-scrollbar bg-white dark:bg-slate-800">
                    <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
                      {SAMPLE_MANIFEST[selectedCategory]?.map(filename => (
                        <label key={filename} className="flex items-center gap-3 p-2.5 sm:p-3 hover:bg-gray-50 dark:hover:bg-slate-700 rounded-lg cursor-pointer text-xs font-mono transition-colors border border-transparent hover:border-gray-200 dark:hover:border-slate-600">
                          <input 
                            type="checkbox" 
                            checked={selectedFiles.has(filename)} 
                            onChange={() => toggleFile(filename)} 
                            className="rounded text-blue-600 focus:ring-blue-500 w-4 h-4 cursor-pointer"
                          />
                          <span className="truncate text-gray-700 dark:text-slate-300">{filename}</span>
                        </label>
                      ))}
                    </div>
                 </div>
                 
                 <div className="p-4 border-t border-gray-100 dark:border-slate-700 bg-gray-50 dark:bg-slate-900/50 shrink-0">
                    <button 
                      onClick={handleLoadManual} 
                      disabled={selectedFiles.size === 0} 
                      className="w-full bg-gray-800 hover:bg-gray-900 dark:bg-slate-700 dark:hover:bg-slate-600 disabled:bg-gray-300 dark:disabled:bg-slate-800 disabled:text-gray-500 text-white px-4 py-3 rounded-lg text-sm font-bold transition-colors shadow-sm"
                    >
                      Queue {selectedFiles.size} Selected Files
                    </button>
                 </div>

              </div>
            </div>
          </div>
        </div>
      )}

      {/* Main Config View */}
      <div className="max-w-2xl w-full bg-white dark:bg-slate-900 border border-gray-200 dark:border-slate-800 rounded-2xl shadow-xl overflow-hidden flex flex-col max-h-full">
        
        {/* Header - Compact Padding */}
        <div className="p-3 sm:p-5 border-b border-gray-100 dark:border-slate-800 flex flex-row justify-between items-center bg-gray-50 dark:bg-slate-900/50 gap-4 shrink-0">
          <div className="flex items-center gap-3 sm:gap-4">
            <button onClick={props.onGoHome} className="p-2 bg-white dark:bg-slate-800 border border-gray-200 dark:border-slate-700 hover:bg-gray-100 dark:hover:bg-slate-700 rounded-lg text-gray-600 dark:text-slate-400 transition-colors shrink-0" title="Return to Home">
              <Home className="w-5 h-5" />
            </button>
            <div className="w-px h-6 bg-gray-300 dark:bg-slate-700 hidden sm:block"></div>
            <h2 className="text-xl sm:text-2xl font-bold flex items-center gap-2">
              <Settings className="w-5 h-5 sm:w-6 sm:h-6 text-blue-500 shrink-0"/> Configuration
            </h2>
          </div>
          <button onClick={props.onToggleTheme} className="p-2 bg-gray-200 dark:bg-slate-800 rounded-lg text-gray-700 dark:text-slate-300 hover:bg-gray-300 dark:hover:bg-slate-700 transition-colors shrink-0">
            {props.theme === 'dark' ? <Sun className="w-4 h-4"/> : <Moon className="w-4 h-4"/>}
          </button>
        </div>
        
        {/* Scrollable Body - Reduced spacing to fit on screen natively */}
        <div className="p-4 sm:p-6 space-y-5 overflow-y-auto custom-scrollbar flex-1 min-h-0">
          
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-4 sm:gap-5">
            <div className="space-y-2">
              <div className="flex justify-between">
                <label className="text-sm font-semibold">Datacenter Nodes</label>
                <span className="text-xs text-gray-500">Max: 250</span>
              </div>
              <ThemeableNumberInput value={props.nodeCount} onChange={props.onNodeChange} onBlur={props.onNodeBlur} min={1} max={250} />
            </div>
            <div className="space-y-2">
              <div className="flex justify-between">
                <label className="text-sm font-semibold">HVAC Ambient (°C)</label>
                <span className="text-xs text-gray-500">Range: 15-45°C</span>
              </div>
              <ThemeableNumberInput value={props.ambientTemp} onChange={props.onTempChange} onBlur={props.onTempBlur} min={15} max={45} />
            </div>
          </div>
          
          <div className="space-y-2 border-t border-gray-100 dark:border-slate-800 pt-4">
            <div className="flex flex-col sm:flex-row sm:justify-between sm:items-center gap-2 mb-2">
              <label className="text-sm font-semibold">Scheduling Policy</label>
              <div className="flex items-center gap-3">
                <div className="group relative flex items-center">
                  <Info className="w-4 h-4 text-gray-400 hover:text-blue-500 transition-colors cursor-help" />
                  <div className="absolute right-0 sm:right-0 top-6 w-[80vw] max-w-xs p-2.5 bg-gray-800 dark:bg-gray-700 text-white text-xs rounded-lg opacity-0 group-hover:opacity-100 transition-opacity pointer-events-none z-10 shadow-xl leading-relaxed">
                    Run both the Standard and Thermal-Aware schedulers side-by-side with identical workloads and identical configuration to compare their thermal efficiency and performance.
                  </div>
                </div>
                <div className="flex items-center gap-2 cursor-pointer group select-none" onClick={() => props.onABTestChange(!props.isABTest)}>
                  <span className={`text-xs font-bold transition-colors ${props.isABTest ? 'text-blue-600 dark:text-blue-400' : 'text-gray-500 dark:text-slate-400 group-hover:text-gray-800 dark:group-hover:text-slate-200'}`}>
                    A/B Testing
                  </span>
                  <button type="button" className={`relative inline-flex h-5 w-9 shrink-0 cursor-pointer rounded-full border-2 border-transparent transition-colors duration-200 ease-in-out focus:outline-none ${props.isABTest ? 'bg-blue-600' : 'bg-gray-300 dark:bg-slate-600'}`} role="switch" aria-checked={props.isABTest}>
                    <span aria-hidden="true" className={`pointer-events-none inline-block h-4 w-4 transform rounded-full bg-white shadow-sm ring-0 transition duration-200 ease-in-out ${props.isABTest ? 'translate-x-4' : 'translate-x-0'}`} />
                  </button>
                </div>
              </div>
            </div>

            <div className={`flex flex-col sm:flex-row bg-gray-100 dark:bg-slate-800 p-1 rounded-lg border border-gray-200 dark:border-slate-700 transition-opacity ${props.isABTest ? 'opacity-50 pointer-events-none' : ''}`}>
              <button onClick={() => props.onModeChange('STANDARD')} className={`flex-1 px-4 py-2 sm:py-2.5 rounded-md text-[13px] sm:text-sm font-bold transition-all ${props.mode === 'STANDARD' ? 'bg-amber-500 text-white shadow' : 'text-gray-500 hover:text-gray-700 dark:text-slate-400'}`}>Standard (Thermal-Unaware)</button>
              <button onClick={() => props.onModeChange('THERMAL_AWARE')} className={`flex-1 px-4 py-2 sm:py-2.5 rounded-md text-[13px] sm:text-sm font-bold transition-all ${props.mode === 'THERMAL_AWARE' ? 'bg-emerald-600 text-white shadow' : 'text-gray-500 hover:text-gray-700 dark:text-slate-400'}`}>Thermal-Aware (ODE)</button>
            </div>
          </div>
          
          <div className="space-y-2 border-t border-gray-100 dark:border-slate-800 pt-4">
            <label className="text-sm font-semibold">Workload Queue</label>
            <div className="flex flex-col items-stretch gap-3">
              
              <div className="flex flex-col sm:flex-row gap-2">
                <label className={`flex-1 flex justify-center items-center gap-2 text-gray-700 dark:text-slate-200 border-2 border-dashed border-gray-300 dark:border-slate-700 px-4 py-2.5 sm:py-2 rounded-lg transition-colors text-center ${props.isUploading ? 'bg-gray-100 dark:bg-slate-800 cursor-not-allowed' : 'hover:bg-gray-50 dark:hover:bg-slate-800 cursor-pointer'}`}>
                  <Upload className="w-4 h-4 shrink-0" /> 
                  <span className="text-[13px] sm:text-sm font-medium">Upload Job Trace (.csv)</span>
                  <input type="file" accept=".csv" multiple className="hidden" onChange={props.onFileUpload} disabled={props.isUploading} />
                </label>

                <button 
                  type="button"
                  onClick={() => setIsSampleModalOpen(true)}
                  disabled={props.isUploading}
                  className={`flex-1 flex justify-center items-center gap-2 text-blue-700 dark:text-blue-400 bg-blue-50 dark:bg-blue-900/30 hover:bg-blue-100 dark:hover:bg-blue-900/50 px-4 py-2.5 sm:py-2 rounded-lg transition-colors text-center ${props.isUploading ? 'opacity-50 cursor-not-allowed' : 'cursor-pointer'}`}
                >
                  <Folder className="w-4 h-4 shrink-0" />
                  <span className="text-[13px] sm:text-sm font-medium">Browse Sample Jobs</span>
                </button>
              </div>
              
              <div className="flex flex-col w-full gap-1 mt-1">
                {props.isUploading ? (
                  <>
                    <div className="flex justify-between items-end text-xs text-gray-500 dark:text-slate-400 font-medium px-1">
                      <span>Processing {props.uploadStats.current} of {props.uploadStats.total} files</span>
                      <span className="font-bold text-blue-600 dark:text-blue-400">{percent}%</span>
                    </div>
                    <div className="w-full bg-gray-200 dark:bg-slate-800 rounded-full h-2.5 overflow-hidden">
                      <div className="bg-blue-600 h-2.5 rounded-full transition-all duration-100" style={{ width: `${percent}%` }}></div>
                    </div>
                  </>
                ) : (
                  <span className="text-xs sm:text-sm font-medium bg-blue-50 dark:bg-blue-900/30 text-blue-700 dark:text-blue-400 px-3 py-1.5 sm:py-1 rounded-full text-center shadow-inner border border-blue-100 dark:border-blue-800">
                    {props.jobCount} Jobs Ready
                  </span>
                )}
              </div>

            </div>
          </div>
        </div>
        
        {/* Footer */}
        <div className="p-3 sm:p-4 bg-gray-50 dark:bg-slate-900/50 border-t border-gray-100 dark:border-slate-800 shrink-0">
          <button onClick={props.onLaunch} className="w-full bg-blue-600 hover:bg-blue-700 text-white font-bold py-2.5 sm:py-3 px-4 rounded-xl shadow-md transition-colors text-sm sm:text-base">
            Initialize & Launch Dashboard
          </button>
        </div>

      </div>
    </div>
  );
}
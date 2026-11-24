import WebSocket, { WebSocketServer } from "ws";
import { GoogleGenAI, Modality } from "@google/genai";
import { Firestore } from "@google-cloud/firestore";
import crypto from "crypto";

/* ======================================================
   Setup

   Transcription runs on the Gemini Live API, which punctuates as
   it transcribes (and in SMART mode also removes disfluencies; see
   TRANSCRIPTION_MODE for why VERBATIM is the default). That is what
   the onBatchCreated polish hop used to do, so this build skips it:
   finals go straight to speech_sentences and onSentenceCreated
   translates them.

   Nothing is written to speech_transcripts, deliberately. That
   collection triggers onFragmentCreated -> session_buffers ->
   fragment_batches -> onBatchCreated, so writing to it would run
   the polish path in parallel and duplicate every sentence. The
   session-start marker speaker.html writes there still feeds the
   listener's speaker dropdown.

   Flow: audio -> Gemini (SMART) -> speech_sentences -> translate.
   The buffering layer, the Cloud Tasks flush and the polish call
   are all out of the path.

   The client protocol is unchanged.

   Deploy:
   gcloud run deploy rosettacast-gemlivesmart --source . --region us-central1 \
     --allow-unauthenticated --set-secrets GEMINI_API_KEY=GEMINI_API_KEY:latest
====================================================== */

const ai = new GoogleGenAI({ apiKey: process.env.GEMINI_API_KEY });
const db = new Firestore({ projectId: "rosettacast" });

const PORT = process.env.PORT || 8080;
const wss = new WebSocketServer({ port: PORT });

const MODEL = "gemini-3.5-transcribe-live";

// SMART: disfluency removal, light grammar cleanup, punctuation -- what the
// polish hop used to do. VERBATIM: raw. Flip per revision without rebuilding:
//   gcloud run services update ... --set-env-vars TRANSCRIPTION_MODE=SMART
//
// VERBATIM is the default since Nov 2026. From about Nov 14 SMART began
// returning each final as the whole session's transcript so far rather than
// the latest turn, so every final repeated the talk from the top until the
// next session rotation (65-77% of its output repeated, Korean and English
// alike). VERBATIM still finalises one turn at a time, and its accuracy on
// conference talks matched what SMART would give with the repeats removed
// (~6% WER in English). Switch back only once SMART finals are per-turn again.
const TRANSCRIPTION_MODE = process.env.TRANSCRIPTION_MODE === "SMART" ? "SMART" : "VERBATIM";

// With no languageCodes at all the API transcribes any language correctly but
// reports "en" for every transcript, which would send the wrong sourceLang
// downstream. Supplying the languages we actually support as hints lets it say
// which one it heard.
const AUTO_LANGUAGE_HINTS = ["en-US", "ko-KR", "es-ES", "fr-FR", "de-DE"];

// Live transcription sessions cap at 10 minutes, so rotate before that.
// Overridable so the rotation can be exercised on a short interval in testing.
const SESSION_ROTATE_MS = Number(process.env.SESSION_ROTATE_MS ?? 9 * 60 * 1000);
const MAX_ROTATIONS = 30;
const MAX_SESSION_MS = 130 * 60 * 1000;
const ROTATE_GRACE_MS = 2500;      // let the outgoing session emit its last final
const MAX_QUEUED_CHUNKS = 200;     // ~13s of audio held while sessions swap
const STUCK_HOLDS = 2;             // synthetic pauses in a row with no final before giving up on a session
const STUCK_COOLDOWN_MS = 20000;   // at most one stuck-session swap this often

// The model only finalises a transcript when it hears a pause, so a speaker in
// full flow can go a long time producing nothing for the pipeline to translate.
//
// What gets committed is chosen by queue depth rather than by the clock.
// Measured over two Korean conference talks, comparing what would have been
// committed against the authoritative final:
//
//   sentences behind it   avg wrong words   committed perfectly
//            0                 4.60                24%
//            1                 4.26                31%
//            2                 1.48                52%
//            3 or more         0.32                79%
//
// The newest sentence is the one still being revised; once three sit behind it
// the oldest is all but settled. So the oldest is committed only when
// SENTENCE_QUEUE of them have piled up behind it. The timers below are
// fallbacks for speech that never reaches that depth.
const SENTENCE_QUEUE = Number(process.env.SENTENCE_QUEUE ?? 3);
// These are dormant safety nets, not part of normal operation. Replaying both
// captured talks with and without them changed nothing on one and bought a
// single second on the other, at the cost of turning a queue release into a
// worse timed one -- so they sit far enough out that ordinary speech, whose
// longest observed silence was 26s, never reaches them. They exist because the
// queue needs sentence ends to fill: a speaker who produces none, or produces
// them very slowly while never pausing, would otherwise leave the screen
// frozen indefinitely, and unbounded is the one thing this must not be.
const FORCE_FLUSH_MS = Number(process.env.FORCE_FLUSH_MS ?? 45000);
const HARD_FLUSH_MS = Number(process.env.HARD_FLUSH_MS ?? 60000);
const FLUSH_TAIL_WORDS = 4;        // never commit the growing edge
const MIN_FLUSH_WORDS = 4;         // a short complete sentence is still worth sending

/* ======================================================
   Vocabulary
   Was speechContexts.phrases with boost 15 under Google STT.
   customVocabulary is the Live API equivalent (cap 1000 terms).
====================================================== */
const VOCABULARY = [

  // ===== SHARED -- identical spelling in English, Spanish and French =====
  // First Presidency and Quorum of the Twelve (March 2026)
  "Dallin H. Oaks", "Henry B. Eyring", "D. Todd Christofferson", "Dieter F. Uchtdorf",
  "David A. Bednar", "Quentin L. Cook", "Neil L. Andersen", "Ronald A. Rasband", "Gary E. Stevenson",
  "Dale G. Renlund", "Gerrit W. Gong", "Ulisses Soares", "Patrick Kearon", "Gérald Caussé",
  "Clark G. Gilbert",
  // Presidency of the Seventy and Presiding Bishopric
  "Carl B. Cook", "Marcus B. Nash", "Michael T. Ringwood", "Arnulfo Valenzuela", "Edward Dube",
  "Kevin R. Duncan", "Benjamin M. Z. Tai", "W. Christopher Waddell", "L. Todd Budge", "Sean Douglas",
  // Presidents of the Church, in order
  "Joseph Smith", "Brigham Young", "John Taylor", "Wilford Woodruff", "Lorenzo Snow",
  "Joseph F. Smith", "Heber J. Grant", "George Albert Smith", "David O. McKay",
  "Joseph Fielding Smith", "Harold B. Lee", "Spencer W. Kimball", "Ezra Taft Benson",
  "Howard W. Hunter", "Gordon B. Hinckley", "Thomas S. Monson", "Russell M. Nelson",
  // Founding figures and witnesses
  "Hyrum Smith", "Emma Smith", "Oliver Cowdery", "Jeffrey R. Holland", "Martin Harris",
  "David Whitmer", "Sidney Rigdon", "Parley P. Pratt", "Orson Pratt",
  // Scripture names and places
  "Alma", "Amulek", "Lehi", "Moroni", "Mormon", "Mosiah", "Korihor", "Zarahemla", "Cumorah",
  "Lamanite", "Mahonri", "Manti", "Gadianton", "Shiz",
  // ENGLISH
  "RosettaCast", "hallelujah", "amen", "Nephi", "Helaman", "Teancum", "Enos", "Ether", "Nephite",
  "Eyring", "Uchtdorf", "Bednar", "Kearon", "Caussé", "Soares", "Christofferson", "Chigbundu",
  "Seventy", "Seventies", "Solemn Assembly", "Sustaining", "Common Consent", "First Presidency",
  "Quorum", "Bishopric", "Stake", "Ward", "Presiding Bishopric", "Relief Society", "Tithing",
  "Fast Offering", "temple recommend", "BYU–Idaho", "Liahona", "Gospel Library", "Melchizedek",
  "Aaronic", "Priesthood", "Firstlings", "Covenant Path", "Plan of Salvation", "Plan of Happiness",
  "Degrees of Glory", "Celestial", "Terrestrial", "Telestial", "Pre-mortal", "Spirit World",
  "Dispensation", "Restoration", "Apostasy", "Spirit Paradise", "Spirit Prison", "Vicarious",
  "Endowment", "Sealing", "Exaltation", "Word of Wisdom", "Family History", "Gathering of Israel",
  "Mortal Probation", "Tabernacle", "Temple Square", "Conference Center", "Redeemer of Israel",
  "Emeritus", "Deseret", "Kirtland", "Nauvoo", "Kolob", "Urim", "President Reiner", "President Nelson",
  "President Monson", "President Hinckley", "President Oaks", "President Eyring", "President Uchtdorf",
  "Elder Holland", "Elder Bednar", "the Prophet Joseph", "Brother Joseph",
  // SPANISH
  "Dios", "Jesucristo", "Espíritu Santo", "iglesia", "Plan de Salvación", "Plan de Felicidad",
  "Sacerdocio de Melquisedec", "Sacerdocio Aarónico", "Sacerdocio", "Senda del Convenio",
  "Recomendación para el Templo", "Sociedad de Socorro", "Obispado Presidente", "Conferencia General",
  "Diezmo", "Palabra de Sabiduría", "Grados de Gloria", "Reino Celestial", "Reino Terrestre",
  "Reino Telestial", "Mundo de los Espíritus", "Paraíso de los Espíritus", "Prisión de los Espíritus",
  "Investidura", "Sellamiento", "Estaca", "Barrio", "Quórum de los Doce Apóstoles",
  "Primera Presidencia", "Asamblea Solemne", "Nefi", "Mormón", "Mosíah", "Helamán", "Teáncum", "Enós",
  "Éter", "Lamanita", "Nefita", "Gadiantón", "José Smith", "José F. Smith", "José Fielding Smith",
  "presidente Nelson", "presidente Monson", "presidente Hinckley", "presidente Oaks",
  "presidente Eyring", "el profeta José Smith",
  // FRENCH
  "Dieu", "Jésus-Christ", "Saint-Esprit", "église", "Plan du Salut", "Plan du Bonheur",
  "Prêtrise de Melchisédek", "Prêtrise d'Aaron", "Prêtrise", "Chemin des Alliances",
  "Recommandation pour le Temple", "Société de Secours", "Épiscopat Président", "Conférence Générale",
  "Dîme", "Parole de Sagesse", "Degrés de Gloire", "Royaume Céleste", "Royaume Terrestre",
  "Royaume Telestiel", "Monde des Esprits", "Paradis des Esprits", "Prison des Esprits", "Dotation",
  "Scellement", "Pieu", "Paroisse", "Collège des douze apôtres", "Première Présidence",
  "Assemblée Solennelle", "Néphi", "Léhi", "Hélaman", "Téancum", "Énos", "Éther", "Néphite",
  "président Nelson", "président Monson", "président Hinckley", "président Oaks", "président Eyring",
  "le prophète Joseph Smith",
  // KOREAN
  "하나님", "예수님", "성령", "교회", "말씀", "구원", "은혜", "구원의 계획", "행복의 계획", "성전 추천서", "멜기세덱 신권", "아론 신권", "신권",
  "성약의 길", "말일성도", "연차대회", "십일조", "지혜의 말씀", "해등영광", "월등영광", "별등영광", "영의 세계", "지상 생활", "회복", "엔다우먼트",
  "인봉", "침례", "십이사도 정원회", "상호부조회", "스테이크", "와드", "앨마", "앰율레크", "니파이", "리하이", "모로나이", "몰몬", "모사이야",
  "힐라맨", "코리호어", "티앤쿰", "제라헤믈라", "쿠모라", "이노스", "이더", "레이맨인", "니파이인", "마혼라이", "맨타이", "가디앤톤", "쉬즈",
  "댈린 에이치 오크스", "헨리 비 아이링", "디 토드 크리스토퍼슨", "디터 에프 우흐트도르프", "데이비드 에이 베드나", "쿠엔틴 엘 쿡", "닐 엘 앤더슨",
  "로널드 에이 래스번드", "게리 이 스테이번슨", "데일 지 런랜드", "게릿 더블유 공", "울리세스 소아레스", "패트릭 키어론", "제랄드 코세", "클라크 지 길버트",
  // Korean -- Presidents of the Church
  "조셉 스미스", "브리검 영", "존 테일러", "윌포드 우드럽", "로렌조 스노우", "조셉 에프 스미스", "히버 제이 그랜트", "조지 앨버트 스미스",
  "데이비드 오 맥케이", "조셉 필딩 스미스", "해롤드 비 리", "스펜서 더블유 킴볼", "에즈라 태프트 벤슨", "하워드 더블유 헌터", "고든 비 힝클리",
  "토마스 에스 몬슨", "러셀 엠 넬슨",
  // Korean -- founding figures
  "하이럼 스미스", "에머 스미스", "올리버 카우드리", "제프리 알 홀런드", "마틴 해리스", "데이비드 휘트머", "시드니 리그돈", "팔리 피 프랫",
  // Korean -- Seventy and Presiding Bishopric
  "칼 비 쿡", "마커스 비 내시", "마이클 티 링우드", "아눌포 발렌수엘라", "에드워드 두베", "케빈 알 던컨", "벤자민 엠 지 타이", "더블유 크리스토퍼 워델",
  "엘 토드 버지", "션 더글라스",
  // Korean -- how leaders are addressed
  "넬슨 회장", "몬슨 회장", "힝클리 회장", "오크스 회장", "아이링 회장", "선지자 조셉 스미스", "십이사도", "칠십인", "감독회",

  // ENGLISH -- worship, ordinances and organisation
  "The Church of Jesus Christ of Latter-day Saints", "Latter-day Saint", "Latter-day Saints",
  "general conference", "semiannual conference", "General Authority", "Area Seventy",
  "Quorum of the Seventy", "elders quorum", "presidency", "priesthood power", "priesthood authority",
  "priesthood keys", "priesthood ordinance", "covenant", "covenants", "temple covenants",
  "make covenants", "keep our covenants", "ministering", "temple ordinance", "temple worship",
  "sealing power", "ordinance", "saving ordinance", "proxy ordinance", "baptism for the dead",
  "confirmation", "gift of the Holy Ghost", "sacrament", "sacrament meeting", "laying on of hands",
  "setting apart", "calling", "called of God", "release", "patriarch", "patriarchal blessing",
  "bishop", "stake president", "stake presidency", "branch", "mission", "mission president",
  "missionary", "full-time missionary", "missionary work", "Primary", "Young Women", "Young Men",
  "Sunday School", "seminary", "institute of religion",

  // SPANISH -- worship, ordinances and organisation
  "La Iglesia de Jesucristo de los Santos de los Últimos Días", "Santos de los Últimos Días",
  "Autoridad General", "cuórum de élderes", "presidencia",
  "poder del sacerdocio", "autoridad del sacerdocio", "llaves del sacerdocio",
  "ordenanza del sacerdocio", "convenio", "convenios del templo", "hacer convenios",
  "guardar los convenios", "ministrar", "ordenanzas del templo", "adoración en el templo",
  "poder sellador", "ordenanza", "ordenanza de salvación", "ordenanza vicaria",
  "bautismo por los muertos", "confirmación", "don del Espíritu Santo", "Santa Cena",
  "reunión sacramental", "imposición de manos", "apartamiento", "llamamiento", "relevo", "patriarca",
  "bendición patriarcal", "obispo", "presidente de estaca", "presidencia de estaca", "rama", "misión",
  "presidente de misión", "misionero", "misionero de tiempo completo", "obra misional", "Primaria",
  "Mujeres Jóvenes", "Hombres Jóvenes", "Escuela Dominical", "seminario", "instituto",

  // FRENCH -- worship, ordinances and organisation
  "L'Église de Jésus-Christ des Saints des Derniers Jours", "saints des derniers jours",
  "autorité générale", "collège des anciens", "présidence",
  "pouvoir de la prêtrise", "autorité de la prêtrise", "clés de la prêtrise",
  "ordonnance de la prêtrise", "alliance", "alliances du temple", "faire des alliances",
  "garder nos alliances", "ordonnances du temple", "culte au temple", "pouvoir de scellement",
  "ordonnance", "ordonnance du salut", "ordonnance par procuration", "baptême pour les morts",
  "don du Saint-Esprit", "Sainte-Cène", "réunion de Sainte-Cène", "imposition des mains",
  "mise à part", "appel", "patriarche", "bénédiction patriarcale", "évêque", "président de pieu",
  "présidence de pieu", "branche", "président de mission", "missionnaire",
  "missionnaire à plein temps", "œuvre missionnaire", "Primaire", "Jeunes Filles", "Jeunes Gens",
  "École du Dimanche", "séminaire", "institut",

  // KOREAN -- worship, ordinances and organisation
  "예수 그리스도 후기 성도 교회", "후기 성도", "연차 대회", "반연차 대회", "총관리 역원", "지역 칠십인", "본부 역원", "제일회장단", "칠십인 정원회",
  "장로 정원회", "신권 권능", "신권 권세", "신권 열쇠", "신권 의식", "정원회", "회장단", "성약", "성전 성약", "성약을 맺다", "성약을 지키다", "성역",
  "성역을 행하다", "성전", "성전 의식", "성전 예배", "인봉 권능", "의식", "구원 의식", "대리 의식", "죽은 자를 위한 침례", "확인", "성신의 은사",
  "성찬", "성찬식", "안수", "성별", "부름", "부름받다", "지지", "해임", "축복사", "축복사의 축복", "감독", "감독단", "스테이크 회장",
  "스테이크 회장단", "지부", "선교부", "선교부 회장", "선교사", "전임 선교사", "선교 사업", "초등회", "청녀", "청남", "주일학교", "세미나리",
  "종교 교육원",

  // ENGLISH -- doctrine, scripture and meetings
  "Book of Mormon", "Doctrine and Covenants", "Pearl of Great Price", "Articles of Faith",
  "Old Testament", "New Testament", "First Vision", "Sacred Grove", "Palmyra", "Zion", "New Jerusalem",
  "dispensation of the fulness of times", "Great Apostasy", "fulness of the gospel",
  "Restoration of the priesthood", "Title of Liberty", "brass plates", "gold plates",
  "Urim and Thummim", "Ammon", "Jacob", "King Benjamin", "Laman", "Lemuel", "Jarom", "Omni",
  "Words of Mormon", "premortal life", "Second Coming", "house of Israel", "enduring to the end",
  "law of consecration", "law of chastity", "Sabbath day", "personal revelation",
  "promptings of the Spirit", "stake conference", "ward council", "high council",
  "ministering brother", "ministering sister", "fast and testimony meeting", "testimony meeting",
  "family history work", "eternal family", "eternal marriage", "Heavenly Father", "Only Begotten Son",
  "Savior", "Redeemer", "infinite Atonement", "Atonement of Jesus Christ", "the veil",
  "kingdoms of glory",

  // SPANISH -- doctrine, scripture and meetings
  "Libro de Mormón", "Doctrina y Convenios", "Perla de Gran Precio", "Artículos de Fe",
  "Antiguo Testamento", "Nuevo Testamento", "Primera Visión", "Arboleda Sagrada", "Sión",
  "Nueva Jerusalén", "dispensación del cumplimiento de los tiempos", "Gran Apostasía",
  "plenitud del evangelio", "restauración del sacerdocio", "Estandarte de la Libertad",
  "planchas de bronce", "planchas de oro", "Urim y Tumim", "Amón", "rey Benjamín", "Lamán",
  "Palabras de Mormón", "vida premortal", "Segunda Venida", "casa de Israel",
  "perseverar hasta el fin", "ley de consagración", "ley de castidad", "día de reposo",
  "revelación personal", "susurros del Espíritu", "conferencia de estaca", "consejo de barrio",
  "sumo consejo", "hermano ministrante", "hermana ministrante", "reunión de ayuno y testimonio",
  "reunión de testimonios", "historia familiar", "familia eterna", "matrimonio eterno",
  "Padre Celestial", "Hijo Unigénito", "Salvador", "Redentor", "Expiación infinita",
  "Expiación de Jesucristo", "el velo",

  // FRENCH -- doctrine, scripture and meetings
  "Livre de Mormon", "Doctrine et Alliances", "Perle de Grand Prix", "Articles de foi",
  "Ancien Testament", "Nouveau Testament", "Première Vision", "Bois Sacré", "Sion",
  "Nouvelle Jérusalem", "dispensation de la plénitude des temps", "Grande Apostasie",
  "plénitude de l'Évangile", "Rétablissement", "rétablissement de la prêtrise",
  "étendard de la liberté", "plaques d'airain", "plaques d'or", "Urim et Thummim", "roi Benjamin",
  "Lémuel", "Paroles de Mormon", "vie prémortelle", "Seconde Venue", "maison d'Israël",
  "persévérer jusqu'à la fin", "loi de consécration", "loi de chasteté", "jour du sabbat",
  "révélation personnelle", "inspirations de l'Esprit", "conférence de pieu", "conseil de paroisse",
  "grand conseil", "réunion de jeûne et de témoignage", "histoire familiale", "famille éternelle",
  "mariage éternel", "Père céleste", "Fils unique", "Sauveur", "Rédempteur", "Expiation infinie",
  "Expiation de Jésus-Christ", "le voile",

  // KOREAN -- doctrine, scripture and meetings
  "복음의 회복", "회복된 복음", "회복된 교회", "회복된 예수 그리스도의 복음", "구속의 계획", "하나님의 계획", "하나님 아버지", "하늘 아버지", "독생자",
  "구주", "구속주", "중보자", "예수 그리스도의 속죄", "무한한 속죄", "승영", "영생", "불멸", "필멸", "필멸의 삶", "전세", "전세의 삶",
  "하나님의 면전", "둘째 오심", "재림", "이스라엘", "이스라엘의 집", "이스라엘의 집합", "이스라엘을 모으다", "휘장", "해의 왕국", "달의 왕국",
  "별의 왕국", "영원한 가족", "영원한 결혼", "영원한 생명", "영원한 진리", "영원한 관점", "제자 됨", "그리스도의 제자", "성신", "성신의 동반",
  "성신의 속삭임", "성신의 인도", "개인 계시", "간증", "간증하다", "개종", "회개", "예수 그리스도를 믿는 신앙", "끝까지 견디다", "회개의 은사", "합당함",
  "합당한", "성결", "헌납", "헌납의 법", "순결의 법", "금식 헌금", "안식일", "안식일을 거룩하게 지키다", "몰몬경", "교리와 성약", "값진 진주",
  "구약전서", "신약전서", "신앙개조", "경전", "경전 구절", "성구", "선지자", "살아 있는 선지자", "후기의 선지자", "사도", "첫번째 시현",
  "첫번째 시현의 숲", "성스러운 숲", "조셉 스미스 이세", "암몬", "야곱", "베냐민 왕", "레이맨", "레뮤엘", "자유의 기치", "놋쇠판", "금판",
  "우림과 둠밈", "나부", "커틀랜드", "팔마이라", "시온", "새 예루살렘", "경륜의 시대", "때가 찬 경륜의 시대", "배도", "대배도", "복음의 충만함",
  "신권의 회복", "니파이전서", "니파이후서", "야곱서", "이노스서", "예이롬서", "옴나이서", "몰몬의 말씀", "모사이야서", "앨마서", "힐라맨서", "제삼니파이",
  "제사니파이", "몰몬서", "이더서", "모로나이서", "스테이크 대회", "와드 대회", "지도자 모임", "와드 평의회", "스테이크 평의회", "고등 평의회",
  "고등평의원", "성전 및 가족 역사", "가족 역사", "가족 역사 사업", "성전 및 가족 역사 사업", "가족 역사 상담자", "성역 형제", "성역 자매", "교회 회원",
  "활동 회원", "회원 기록", "개종자", "새로운 회원", "관심 있는 사람", "침례식", "확인 의식", "성별하다", "안수하다", "지지하다", "해임하다",
  "부름을 받다", "부름을 수행하다", "감리하다", "감리 역원", "개회 기도", "폐회 기도", "개회 찬송", "폐회 찬송", "간증 모임", "금식 간증 모임",

  // ENGLISH -- from the supplied list
  "semiannual general conference", "Quorum of the Twelve Apostles", "General Authorities",
  "General Officer", "district", "temple", "house of the Lord", "Aaronic Priesthood",
  "Melchizedek Priesthood", "covenant relationship", "covenant people", "covenant keeping",
  "covenant-keeping", "temple covenant", "keep covenants", "temple ordinances", "initiatory",
  "sealing ordinance", "ordinances", "saving ordinances", "ordinances of salvation",
  "ordinances of exaltation", "proxy baptism", "sustain", "released", "institute",

  // KOREAN -- honorific forms heard in real speech
  "감독님", "감독", "감독 회장님", "회장님", "자매님", "형제님", "장로님",

  // KOREAN -- equivalents for the new concepts
  "예비 의식", "주의 집", "지방부", "성약의 백성", "구원의 의식", "성전 의식들", "대리 침례", "총관리 역원들",

  // SPANISH -- equivalents for the new concepts
  "la casa del Señor", "distrito", "pueblo del convenio", "ordenanzas de salvación",
  "ordenanzas de exaltación", "bautismo vicario", "Autoridades Generales",
  "conferencia general semestral",

  // FRENCH -- equivalents for the new concepts
  "la maison du Seigneur", "peuple de l'alliance", "ordonnances du salut",
  "ordonnances de l'exaltation", "baptême par procuration", "autorités générales",
  "conférence générale semestrielle"
];

/* ======================================================
   Helper: which language was actually spoken

   The Live API's languageCode only echoes the configured hint -- it reports
   "en" for fluent Korean -- so it cannot be used to pick a source language.
   Script is decisive where the alphabet differs, so Hangul is settled here for
   free. Latin-script languages are left to Cloud Translation, which detects
   them as part of the translation call it already makes.
====================================================== */
function languageFromScript(text) {
  const letters = (text.match(/\p{L}/gu) ?? []).length;
  if (!letters) return null;
  const hangul = (text.match(/[\uAC00-\uD7AF\u1100-\u11FF\u3130-\u318F]/g) ?? []).length;
  // A Korean sentence quoting an English phrase is still Korean
  if (hangul / letters > 0.3) return "ko";
  return null;
}

/* ======================================================
   Helper: Firestore write

   Writes the shape onBatchCreated used to produce, so
   onSentenceCreated picks it up with no change.
====================================================== */
async function storeSentence({ sessionId, speaker, text, language, langSource }) {
  const now = Date.now();
  const sourceLang = language?.split("-")[0] ?? "en";
  return db.collection("speech_sentences").add({
    sessionId,
    speaker,
    sourceLang,
    // "script" means sourceLang is decided and translation should trust it;
    // "config" means it is only the echoed hint and must be re-detected.
    langSource,
    text,
    rawText: text,          // SMART output is already the clean text
    fragmentCount: 1,
    timestamp: now,
    createdAt: now,
    expiresAt: new Date(now + 24 * 60 * 60 * 1000),
  }).then(() => {
    console.log(`Sentence written | "${text.slice(0, 40)}"`);
  });
}

/* ======================================================
   WebSocket Connection
====================================================== */
wss.on("connection", (ws) => {
  console.log("Client connected");

  // Falls back to a fresh id, but the client supplies a stable one that
  // survives its connection rotations, so a long talk stays one session
  // and sentence-merge context is not reset every time the socket is replaced.
  let sessionId = crypto.randomUUID();
  const openedAt = Date.now();
  let live = null;
  let initialized = false;
  let sessionLanguage = null;
  let sessionSpeaker = "unknown";
  let rotations = 0;
  let rotateTimeout = null;
  let rotating = false;
  let finalCount = 0;
  let lastDetected = null;

  // Forced-flush bookkeeping. Interim text is cumulative for the current
  // utterance, so flushedWords records how much of it has already been
  // committed; the eventual final is sliced past that to avoid repeating it.
  let interimText = "";

  // Synthetic pause. Gemini's interim stream degrades once one utterance runs
  // long, so a speaker who never pauses gets a short pause made for them: live
  // audio is held back and silence sent in its place until Gemini finalises,
  // then the held audio is sent on. Nothing is dropped.
  //
  // Interim text trails the audio by half a second or more, so by the time a
  // period shows up the speaker is already into the next sentence. Two rules
  // keep the cut clean:
  //  - arm only once Gemini has heard words after the period and kept it, so a
  //    period guessed at a mid-sentence pause that later becomes a comma never
  //    triggers a break;
  //  - once armed, cut at the next quiet gap in the audio, never mid-word.
  // The final then ends a few words into the next sentence; those words are
  // carried over and joined to the front of the next final.
  //
  // A cut that lands mid-sentence costs accuracy: Gemini takes the silence for
  // the end of the utterance and can invent an ending for the fragment ("taught
  // me about the Plan of Salvation"). So outside Korean a cut is made only once
  // an utterance runs past LONG_UTTERANCE_CHARS, where the interim would
  // otherwise degrade; speech with ordinary pauses is left alone.
  //
  // Korean works differently. Its grammar marks the end of a sentence
  // (...다. ...요. ...까?), so a period after one of those endings is no guess,
  // and its finals end cleanly there but drop any words trailing past it. So
  // Korean cuts the moment such a sentence closes, with nothing left to carry.
  // Every cut still costs a little accuracy at the seam, so Korean waits until
  // the utterance has run KOREAN_BREAK_CHARS without a final of its own, then
  // pauses at the next sentence ending -- never mid-sentence.
  const FORCE_BREAK_CHARS = Number(process.env.FORCE_BREAK_CHARS ?? 40);   // finished text needed before any cut
  const KOREAN_BREAK_CHARS = Number(process.env.KOREAN_BREAK_CHARS ?? 150);
  const LONG_UTTERANCE_CHARS = Number(process.env.LONG_UTTERANCE_CHARS ?? 300);  // other languages
  const BREAK_AFTER_WORDS = 2;      // words heard past the period before arming
  const MAX_HOLD_MS = 2500;
  const GAP_FRAMES = 6;             // 60ms of quiet: a gap between words, not a consonant
  let armed = false, holding = false, heldAudio = [], holdStartedAt = 0, holdTimer = null, breakCount = 0;
  let carry = "";                   // tail of a forced final, waiting for its sentence to finish
  let confirmed = 0;                // sentences Gemini had already closed when the cut was made
  const levels = [];                // recent 10ms frame levels, for an adaptive quiet threshold
  let quietRun = 0;
  let lastFinal = "";               // Gemini's previous final, which the next interims may repeat
  let afterCut = false;             // the previous final was one we forced
  // Now and then a Gemini session stops finalising altogether: even seconds of
  // silence bring no final. After STUCK_HOLDS pauses in a row like that, the
  // session is swapped for a fresh one.
  let stuckHolds = 0, lastStuckSwap = 0, stuckSwaps = 0;
  let liveGen = 0, ignoredGen = 0;  // messages from an abandoned session are dropped
  let flushedText = "";        // the exact text already committed for this utterance
  let lastEmit = Date.now();
  let flushTimer = null;

  // Audio arriving while sessions swap, so speech is not dropped
  const pending = [];

  const sessionTimeout = setTimeout(() => {
    console.log(`Session timeout | session=${sessionId}`);
    closeLive();
    try { ws.close(); } catch {}
  }, MAX_SESSION_MS);

  function send(payload) {
    try { ws.send(JSON.stringify(payload)); } catch {}
  }

  const wordsOf = (t) => t.trim().split(/\s+/).filter(Boolean);
  const bare = (w) => (w ?? "").toLowerCase().replace(/[^\p{L}\p{N}]/gu, "");

  // What is left of a transcript once the already-committed part is removed.
  // Slicing by word count is not enough: the recogniser rewrites words it has
  // already emitted, so the counts drift and the seam duplicates or loses a
  // word. Anchoring on the tail of the committed text survives those rewrites.
  function remainderAfter(text) {
    if (!flushedText) return text;
    const clean = text.trim();
    if (clean.startsWith(flushedText)) return clean.slice(flushedText.length).trim();
    const committed = wordsOf(flushedText);
    const anchor = committed.slice(-Math.min(3, committed.length)).join(" ");
    const at = clean.lastIndexOf(anchor);
    if (at >= 0) return clean.slice(at + anchor.length).trim();
    return wordsOf(clean).slice(committed.length).join(" ");   // last resort
  }

  function emitSentence(text, why) {
    const clean = text.trim();
    if (!clean) return;
    finalCount++;
    const fromScript = languageFromScript(clean);
    const language = fromScript ?? sessionLanguage ?? "en";
    const langSource = fromScript ? "script" : "config";
    if (language !== lastDetected) {
      console.log(`Source language: ${language} (${langSource}) | session=${sessionId}`);
      lastDetected = language;
    }
    console.log(`Sentence #${finalCount} (${why}) | lang=${language} | "${clean.slice(0, 60)}"`);
    send({ text: clean, isFinal: true, language, speaker: sessionSpeaker });
    storeSentence({
      sessionId, speaker: sessionSpeaker, text: clean, language, langSource,
    }).catch((err) => console.error("Firestore write failed:", err.message));
    lastEmit = Date.now();
  }

  // Split into sentences the model has actually terminated, plus the tail it
  // is still working on.
  function splitSentences(text) {
    const parts = text.split(/(?<=[.!?。！？])\s+/).map((p) => p.trim()).filter(Boolean);
    const complete = [];
    let leftover = "";
    for (const p of parts) {
      // Stray punctuation ("served. . .") is not a sentence of its own
      if (!/[\p{L}\p{N}]/u.test(p)) {
        if (leftover) leftover += " " + p;
        else if (complete.length) complete[complete.length - 1] += " " + p;
        continue;
      }
      if (/[.!?。！？]$/.test(p)) complete.push(p);
      else leftover = leftover ? leftover + " " + p : p;
    }
    return { complete, leftover };
  }

  // Text not yet sent: any carried tail, then whatever the current utterance
  // has added since the last commit.
  function pendingText(text) {
    return joinCarry(remainderAfter(text)).text;
  }

  // The carry joined to what followed it. A cut can leave the same words on
  // both sides of the seam -- whole ("family" / "Family"), clipped ("Sac" /
  // "sacrament") or re-heard as an ending ("계속되었습니다" / "되었습니다") --
  // so the carry gives up whatever the new text repeats. `head` is the carry
  // as it appears in `text`.
  function joinCarry(rest) {
    if (!carry || !rest) return { head: carry, text: carry || rest };
    const head = wordsOf(carry);
    const next = wordsOf(rest).map(bare);
    let k = Math.min(5, head.length, next.length);
    while (k > 0 && head.slice(-k).map(bare).join(" ") !== next.slice(0, k).join(" ")) k--;
    if (k > 0) {
      head.splice(-k);
    } else if (head.length) {
      const last = head[head.length - 1], stub = bare(last), first = next[0];
      if (stub.length >= 2 && first.length > stub.length && first.startsWith(stub)) head.pop();
      else if (first.length >= 2 && stub.length > first.length && last.endsWith(first)) {
        head[head.length - 1] = last.slice(0, last.length - first.length);
      }
    }
    const joined = head.join(" ");
    return { head: joined, text: [joined, rest].filter(Boolean).join(" ") };
  }

  function commitChunk(chunk, why) {
    // A chunk is always the front of pendingText(): it may use up only part of
    // the carry, or all of it and some of the current utterance. Only the part
    // past the carry counts against this utterance's interim text.
    let own = chunk;
    const { head } = joinCarry(remainderAfter(interimText));
    if (head && head.startsWith(chunk)) { carry = head.slice(chunk.length).trim(); own = ""; }
    else if (head && chunk.startsWith(head)) { carry = ""; own = chunk.slice(head.length).trim(); }
    if (own) flushedText = (flushedText + " " + own).trim();
    emitSentence(chunk, why);
  }

  // Primary rule: once more than SENTENCE_QUEUE finished sentences are waiting,
  // the oldest has stopped changing -- send it.
  function drainQueue() {
    if (!interimText) return;
    for (let guard = 0; guard < 10; guard++) {
      const { complete } = splitSentences(pendingText(interimText));
      if (complete.length <= SENTENCE_QUEUE) return;
      console.log(`Queue at ${complete.length} sentences -- releasing the oldest | session=${sessionId}`);
      commitChunk(complete[0], `queue/${complete.length}`);
    }
  }

  // Fallbacks for speech that never fills the queue: a speaker who produces one
  // long sentence, or pauses so rarely that waiting would leave a blank screen.
  function timedFlush() {
    if (!interimText) return;
    const waited = Date.now() - lastEmit;
    if (waited < FORCE_FLUSH_MS) return;

    const remainder = pendingText(interimText);
    const { complete } = splitSentences(remainder);
    if (complete.length >= 1) {
      console.log(`No output for ${(waited / 1000).toFixed(0)}s -- releasing one sentence early | session=${sessionId}`);
      commitChunk(complete[0], "timed/sentence");
      return;
    }

    // Nothing terminated at all: cut mid-sentence, but only as a last resort.
    if (waited < HARD_FLUSH_MS) return;
    const available = wordsOf(remainder);
    const cut = available.length - FLUSH_TAIL_WORDS;
    if (cut < MIN_FLUSH_WORDS) return;
    console.log(`No sentence end for ${(waited / 1000).toFixed(0)}s -- cutting mid-sentence | session=${sessionId}`);
    commitChunk(available.slice(0, cut).join(" "), "timed/mid-sentence");
  }

  /* ---- transcripts coming back from Gemini ---- */
  function handleMessage(message) {
    const sc = message.serverContent;

    // Speculative partial hypothesis - shown to the speaker, never stored
    const interim = sc?.interimInputTranscription;
    if (interim?.text) {
      // Just after a final the interim can still open with that final's text;
      // only what follows it belongs to the new utterance.
      const fresh = lastFinal ? wordsAfter(interim.text, lastFinal, wordsOf(lastFinal).length + 2) : null;
      if (fresh === null) lastFinal = "";
      interimText = fresh ?? interim.text;
      const brk = !armed && !holding && interimText ? shouldBreak(interimText) : null;
      if (brk === "now") startHold();
      else if (brk === "gap") armed = true;
      drainQueue();
      send({
        text: interim.text, isFinal: false,
        language: languageFromScript(interim.text) ?? sessionLanguage ?? "auto",
        speaker: sessionSpeaker,
      });
    }

    // Finalized when the speaker pauses - this is what the pipeline consumes.
    // languageCode is what Gemini actually heard, so the source language is
    // decided per sentence: a Korean talk quoting English scripture translates
    // each sentence from the language it was really spoken in.
    const transcript = sc?.inputTranscription;
    let final = transcript?.text?.trim();
    if (final) {
      stuckHolds = 0;
      const forced = holding;
      if (holding) endHold("final");
      armed = false;
      // The turn after a cut can lose its first words from the final though
      // the interim showed them ("Find on our own. Alma taught..." finalised
      // as "Alma taught..."); put them back.
      if (afterCut) {
        const lead = wordsBefore(interimText, final);
        if (lead && languageFromScript(lead) === languageFromScript(final) && !repeats(lead, lastFinal)) {
          final = lead + " " + final;
        }
      }
      afterCut = forced;
      // Anything already force-flushed is dropped from the front, so a long
      // utterance is not sent twice.
      let text = pendingText(final);
      // Words the interim showed past the end of the final
      const heard = forced ? wordsAfter(interimText, final) : null;
      lastFinal = final;
      carry = "";
      interimText = "";
      flushedText = "";
      lastEmit = Date.now();
      // A final we forced stops wherever the cut fell, usually a few words into
      // the next sentence, and Gemini often puts a period at the cut. Only the
      // sentences it had closed before the cut are trusted; the rest is held
      // over, minus the period the cut invented.
      if (forced) {
        const { complete } = splitSentences(text);
        const keep = Math.min(confirmed, complete.length);
        carry = tailAfter(text, keep);
        text = complete.slice(0, keep).join(" ");
        // The final can drop words the interim had already shown past its end;
        // keep them, unless the interim had drifted into another script.
        if (heard && languageFromScript(heard) === languageFromScript(final)) {
          carry = [carry, heard.replace(/[.!?。！？]+$/, "")].filter(Boolean).join(" ");
        }
      }
      if (text) emitSentence(text, forced ? "break" : "final");
      else if (!carry) console.log(`Final already covered by forced flushes | session=${sessionId}`);
    }

    // Server is about to hang up - get ahead of it
    if (message.goAway) {
      console.log(`GoAway | session=${sessionId} | timeLeft=${message.goAway.timeLeft}`);
      rotate("goAway");
    }
  }

  async function openLive() {
    const gen = ++liveGen;
    return ai.live.connect({
      model: MODEL,
      callbacks: {
        onopen: () => console.log(`Live session opened | session=${sessionId} | rotation #${rotations}`),
        onmessage: (message) => { if (gen > ignoredGen) handleMessage(message); },
        onerror: (err) => {
          console.error(`Live API error | session=${sessionId} | ${err?.message ?? err}`);
          rotate("error");
        },
        onclose: (e) => console.log(`Live session closed | session=${sessionId} | ${e?.reason ?? ""}`),
      },
      config: {
        responseModalities: [Modality.TEXT],
        inputAudioTranscription: {
          languageCodes: sessionLanguage ? [sessionLanguage] : AUTO_LANGUAGE_HINTS,
          customVocabulary: VOCABULARY,
          mode: TRANSCRIPTION_MODE,
        },
      },
    });
  }

  // Once the utterance is long enough: "now" to cut at once (a Korean sentence
  // just closed), "gap" to cut at the next quiet gap (Gemini has kept a period
  // through BREAK_AFTER_WORDS more words), or null to keep listening.
  function shouldBreak(text) {
    const { complete, leftover } = splitSentences(pendingText(text));
    if (!complete.length) return null;
    if (complete.join(" ").length < FORCE_BREAK_CHARS) return null;
    const korean = languageFromScript(text) === "ko";
    if (text.length < (korean ? KOREAN_BREAK_CHARS : LONG_UTTERANCE_CHARS)) return null;
    if (!leftover && closesKorean(complete[complete.length - 1])) return "now";
    // Korean is only ever cut where a sentence has just closed: a cut into the
    // next sentence costs far more accuracy than waiting for the next ending.
    if (korean) return null;
    return wordsOf(leftover).length >= BREAK_AFTER_WORDS ? "gap" : null;
  }

  const closesKorean = (sentence) => /[다요죠까니][.!?]$/.test(sentence ?? "");

  // The words of `text` after the end of `ref`, found by ref's last three
  // words (or two, as the two often differ by a word) ending no later than
  // word `within` of text; null when they are not there.
  function wordsAfter(text, ref, within = Infinity) {
    const t = wordsOf(text), tb = t.map(bare);
    const r = wordsOf(ref).map(bare).filter(Boolean);
    for (let n = Math.min(3, r.length); n >= Math.min(2, r.length) && n > 0; n--) {
      const anchor = r.slice(-n).join(" ");
      for (let i = Math.min(tb.length, within) - n; i >= 0; i--) {
        if (tb.slice(i, i + n).join(" ") === anchor) return t.slice(i + n).join(" ");
      }
    }
    return null;
  }

  // The words of `text` before the start of `ref`, found by ref's first three
  // (or two) words starting within the first `within` words of text.
  function wordsBefore(text, ref, within = 8) {
    const t = wordsOf(text), tb = t.map(bare);
    const r = wordsOf(ref).map(bare).filter(Boolean);
    for (let n = Math.min(3, r.length); n >= Math.min(2, r.length) && n > 0; n--) {
      const anchor = r.slice(0, n).join(" ");
      for (let i = 1; i <= Math.min(within, tb.length - n); i++) {
        if (tb.slice(i, i + n).join(" ") === anchor) return t.slice(0, i).join(" ");
      }
    }
    return "";
  }

  // Whether most of `words` already appear near the end of `ref`: an interim
  // re-hearing the previous turn in different words, not something new.
  function repeats(words, ref) {
    const seen = new Set(wordsOf(ref).slice(-30).map(bare));
    const w = wordsOf(words).map(bare).filter(Boolean);
    return w.length > 0 && w.filter((x) => seen.has(x)).length / w.length >= 0.5;
  }

  // Words after the first `confirmed` sentences, minus any period a cut put on
  // the end.
  function tailAfter(text, n) {
    const { complete, leftover } = splitSentences(text);
    return [...complete.slice(n), leftover].filter(Boolean).join(" ").replace(/[.!?。！？]+$/, "");
  }

  // RMS of each 10ms frame of 16-bit mono PCM at 16kHz
  function frameLevels(chunk) {
    const out = [];
    for (let f = 0; f + 320 <= chunk.length; f += 320) {
      let sum = 0;
      for (let i = f; i < f + 320; i += 2) { const v = chunk.readInt16LE(i); sum += v * v; }
      out.push(Math.sqrt(sum / 160));
    }
    return out;
  }

  // Quiet relative to this speaker and this room: near the floor of the last
  // few seconds, well below its speech.
  function quietThreshold() {
    const sorted = [...levels].sort((a, b) => a - b);
    const lo = sorted[Math.floor(sorted.length * 0.1)] ?? 0;
    const hi = sorted[Math.floor(sorted.length * 0.9)] ?? 0;
    return Math.max(lo * 2, lo + (hi - lo) * 0.1);
  }

  function sendAudio(chunk) {
    if (holding) {
      heldAudio.push(chunk);
      sendToGemini(Buffer.alloc(chunk.length));  // Gemini hears silence instead
      return;
    }
    const frames = frameLevels(chunk);
    const threshold = quietThreshold();
    let cutAt = -1;
    frames.forEach((level, i) => {
      quietRun = level <= threshold ? quietRun + 1 : 0;
      if (armed && cutAt < 0 && quietRun >= GAP_FRAMES && levels.length >= 100) cutAt = (i + 1) * 320;
    });
    levels.push(...frames);
    if (levels.length > 300) levels.splice(0, levels.length - 300);

    if (cutAt < 0) { sendToGemini(chunk); return; }
    // Send up to and including the gap, then hold everything after it.
    sendToGemini(chunk.subarray(0, cutAt));
    startHold();
    if (cutAt < chunk.length) sendAudio(chunk.subarray(cutAt));
  }

  function startHold() {
    armed = false;
    // Periods with words after them (or after a Korean sentence ending) were
    // heard as sentence ends, not guessed.
    const { complete, leftover } = splitSentences(pendingText(interimText));
    const closed = !leftover && closesKorean(complete[complete.length - 1]);
    confirmed = leftover || closed ? complete.length : complete.length - 1;
    holding = true;
    heldAudio = [];
    holdStartedAt = Date.now();
    breakCount++;
    console.log(`Synthetic pause #${breakCount} | session=${sessionId}`);
    holdTimer = setTimeout(() => endHold("timeout"), MAX_HOLD_MS);
  }

  function endHold(why) {
    if (!holding) return;
    clearTimeout(holdTimer);
    holding = false;
    const held = heldAudio;
    heldAudio = [];
    const ms = Date.now() - holdStartedAt;
    console.log(`Synthetic pause #${breakCount} released (${why}) after ${ms}ms | session=${sessionId}`);

    stuckHolds = why === "timeout" ? stuckHolds + 1 : 0;
    if (stuckHolds >= STUCK_HOLDS && Date.now() - lastStuckSwap >= STUCK_COOLDOWN_MS && live && !rotating) {
      abandonStuckSession();
      rotate("stuck");      // takes `live` away at once, so the held audio below queues for the new session
    }
    for (const c of held) sendToGemini(c);
  }

  // The session has stopped finalising, so its interim is all this utterance
  // will ever get: send its finished sentences now, carry the unfinished one
  // into the next session, and stop listening to the old one.
  function abandonStuckSession() {
    stuckHolds = 0;
    lastStuckSwap = Date.now();
    stuckSwaps++;
    ignoredGen = liveGen;
    const { complete, leftover } = splitSentences(pendingText(interimText));
    carry = "";
    if (complete.length) emitSentence(complete.join(" "), "stuck");
    carry = leftover;
    interimText = "";
    flushedText = "";
    lastFinal = "";
    afterCut = false;
    armed = false;
    console.log(`Gemini stopped finalising -- swapping in a fresh session (#${stuckSwaps}) | session=${sessionId}`);
  }

  function sendToGemini(chunk) {
    if (!live) {
      if (pending.length < MAX_QUEUED_CHUNKS) pending.push(chunk);
      return;
    }
    try {
      live.sendRealtimeInput({
        audio: { data: chunk.toString("base64"), mimeType: "audio/pcm;rate=16000" },
      });
    } catch (err) {
      console.error(`Audio send failed | session=${sessionId} | ${err.message}`);
    }
  }

  function flushPending() {
    while (pending.length && live) sendToGemini(pending.shift());
  }

  function closeLive(session = live) {
    if (!session) return;
    try { session.sendRealtimeInput({ audioStreamEnd: true }); } catch {}
    try { session.close(); } catch {}
  }

  /* ---- swap in a fresh session before the 10 minute cap ---- */
  async function rotate(reason) {
    if (rotating || ws.readyState !== WebSocket.OPEN) return;
    clearTimeout(rotateTimeout);

    if (rotations >= MAX_ROTATIONS) {
      console.log(`Max rotations reached | session=${sessionId}`);
      closeLive();
      try { ws.close(); } catch {}
      return;
    }

    rotating = true;
    // A stuck-session swap is a repair, not the scheduled rotation, so it does
    // not use up the session's MAX_ROTATIONS
    if (reason !== "stuck") rotations++;
    console.log(`Rotating live session #${rotations} | session=${sessionId} | reason=${reason}`);

    const outgoing = live;
    live = null;                     // audio queues while the new session comes up

    try {
      live = await openLive();
      flushPending();
    } catch (err) {
      // Let the client's own reconnect logic take over rather than limping along
      console.error(`Rotation failed | session=${sessionId} | ${err.message}`);
      rotating = false;
      try { ws.close(1011, "live_session_failed"); } catch {}
      return;
    }
    rotating = false;

    if (outgoing) {
      try { outgoing.sendRealtimeInput({ audioStreamEnd: true }); } catch {}
      setTimeout(() => { try { outgoing.close(); } catch {} }, ROTATE_GRACE_MS);
    }

    rotateTimeout = setTimeout(() => rotate("scheduled"), SESSION_ROTATE_MS);
  }

  ws.on("message", async (msg) => {
    if (!initialized) {
      let cfg;
      try { cfg = JSON.parse(msg.toString()); } catch {
        console.error("Invalid JSON config");
        ws.close();
        return;
      }

      if (cfg.type !== "config") {
        console.error("Missing config");
        ws.close();
        return;
      }

      // A language is now optional. Omitted, or "auto", means Gemini detects it
      // and reports what it heard with every transcript. A real code is still
      // honoured as a hint, which is the escape hatch if detection misbehaves.
      sessionLanguage = (typeof cfg.language === "string" && cfg.language !== "auto")
        ? cfg.language : null;
      sessionSpeaker = typeof cfg.speaker === "string" && cfg.speaker.length > 0
        ? cfg.speaker : "unknown";
      if (typeof cfg.sessionId === "string" && /^[A-Za-z0-9-]{8,64}$/.test(cfg.sessionId)) {
        sessionId = cfg.sessionId;
      }

      // Mark initialized BEFORE awaiting the connect. The client sends an audio
      // frame every ~64ms, so frames arrive during the handshake; without this
      // they fall into this branch and get parsed as config. They queue in
      // `pending` instead and are flushed once the session is up.
      initialized = true;
      console.log(`Starting Gemini Live | session=${sessionId} | language=${sessionLanguage ?? "auto-detect"} | speaker=${sessionSpeaker}`);

      try {
        live = await openLive();
      } catch (err) {
        console.error(`Could not open live session | ${err.message}`);
        send({ error: "transcription unavailable" });
        ws.close();
        return;
      }

      flushPending();
      rotateTimeout = setTimeout(() => rotate("scheduled"), SESSION_ROTATE_MS);
      flushTimer = setInterval(timedFlush, 1000);
      return;
    }

    sendAudio(msg);
  });

  ws.on("close", (code, reason) => {
    clearTimeout(sessionTimeout);
    clearTimeout(rotateTimeout);
    if (flushTimer) clearInterval(flushTimer);
    const alive = ((Date.now() - openedAt) / 1000).toFixed(0);
    // The close code is the only evidence of why a live session ended, and
    // without it a drop mid-meeting is undiagnosable after the fact.
    console.log(`Client disconnected | session=${sessionId} | code=${code} | reason="${reason ?? ""}" | ` +
                `alive=${alive}s | finals=${finalCount} | rotations=${rotations} | pauses=${breakCount} | stuckSwaps=${stuckSwaps}`);
    clearTimeout(holdTimer);
    if (carry) emitSentence(carry, "carry/close");
    closeLive();
  });

  ws.on("error", (err) => {
    console.error("WebSocket error:", err.message);
    closeLive();
  });
});

console.log(`RosettaCast (Gemini Live ${TRANSCRIPTION_MODE}, ${MODEL}) -> speech_sentences | force-flush ${FORCE_FLUSH_MS}ms | port ${PORT}`);

import type { Metadata } from "next";
import Link from "next/link";
import { ArrowLeft, ExternalLink } from "lucide-react";

export const metadata: Metadata = {
  title: "Chronic Absenteeism GTM Report | California LCAP Intelligence",
  description: "Best California districts to market chronic absenteeism outbound voice calls."
};

type ReportRow = {
  priority: number;
  cdsCode: string;
  district: string;
  absenteeism: string;
  riskRed: string;
  riskOrange: string;
  whyHit: string;
  evidence: string;
  lcapUrl: string;
  superintendent: string[];
  businessContact: string[];
};

const rows: ReportRow[] = [
  {
    priority: 1,
    cdsCode: "48705810000000",
    district: "Vallejo City Unified",
    absenteeism: "33.1% chronic absenteeism; 2,278 chronically absent students; 4 red and 4 orange student-group indicators.",
    riskRed: "HOM 54.3% (208); WH 30.0% (95); AS 23.9% (38); FOS 24.2% (8)",
    riskOrange: "SED 35.6% (2,067); AA 41.7% (546); LTEL 34.4% (72); PI 36.2% (38)",
    whyHit:
      "Best blend of pain, workflow fit, and district size. LCAP has funded attendance awareness, MTSS, CWA/wraparound support, and site family connection work.",
    evidence:
      'Action 2.5 includes "Site Supports to connect with families." Action 3.8 funds wraparound support for chronically absent students.',
    lcapUrl: "https://cdeunifiedstoragewest.blob.core.windows.net/lcaps/4192c810-339c-4902-93cb-6adb7164affa.pdf",
    superintendent: ["Ruben Aurelio, Superintendent", "raurelio@vcusd.org", "(707) 556-8921 Ext. 50002"],
    businessContact: [
      "Ruben Fernandez, Asst. Superintendent Business and Operations",
      "rfernandez@vcusd.org",
      "(707) 556-8921 Ext. 50059"
    ]
  },
  {
    priority: 2,
    cdsCode: "15636770000000",
    district: "Mojave Unified",
    absenteeism: "50.8% chronic absenteeism; 1,174 chronically absent students; 4 orange indicators.",
    riskRed: "none",
    riskOrange: "HOM 58.9% (86); LTEL 48.6% (34); FOS 47.3% (26); AS 45.1% (23)",
    whyHit:
      "Smaller district, but the most explicit product fit. Their LCAP names automated absence calls directly and ties the work to a $3.38M attendance action.",
    evidence: 'Action 2.19: "Automated phone calls will be used to notify parents of absences."',
    lcapUrl: "https://cdeunifiedstoragewest.blob.core.windows.net/lcaps/617b3921-bc21-4736-8b4f-3571bea91770.pdf",
    superintendent: [
      "Dr. Katherine Aguirre, Superintendent",
      "katherineaguirre@mojave.k12.ca.us",
      "(661) 824-4001 Ext. 10502"
    ],
    businessContact: [
      "Cassie Hogan, Assistant Superintendent, Business",
      "cassiehogan@mojave.k12.ca.us",
      "(661) 824-4001 Ext. 10303"
    ]
  },
  {
    priority: 3,
    cdsCode: "36739570000000",
    district: "Snowline Joint Unified",
    absenteeism: "30.2% chronic absenteeism; 1,871 chronically absent students; worsening by 2.5 points.",
    riskRed:
      "SED 33.4% (1,670); HI 31.0% (1,201); SWD 36.4% (409); WH 25.8% (347); EL 30.8% (279); HOM 49.2% (192); AA 40.4% (152); MR 30.1% (143); LTEL 37.6% (56); FOS 30.7% (43)",
    riskOrange: "none",
    whyHit:
      "Very strong direct attendance action: $7.94M for student/family attendance work, attendance specialists, and family partnership. Good fit for automated family outreach layered onto human intervention.",
    evidence: 'Action 2.7 says staff will "partner with families to foster positive attendance."',
    lcapUrl: "https://cdeunifiedstoragewest.blob.core.windows.net/lcaps/a793ce0c-024b-4042-ac4e-d91e062410a9.pdf",
    superintendent: ["Ryan Holman, Superintendent", "ryan_holman@snowlineschools.com", "(760) 868-5817 Ext. 10112"],
    businessContact: ["Bill Flynn, CBO", "bill_flynn@snowlineschools.com", "(760) 868-5817 Ext. 10211"]
  },
  {
    priority: 4,
    cdsCode: "36677100000000",
    district: "Fontana Unified",
    absenteeism: "23.8% chronic absenteeism; 5,359 chronically absent students; worsening by 1.3 points.",
    riskRed:
      "SED 24.8% (4,721); HI 24.0% (4,639); EL 20.2% (1,269); SWD 31.1% (1,124); AA 28.9% (350); MR 24.1% (62); PI 33.3% (14)",
    riskOrange: "LTEL 22.7% (244); WH 21.4% (191); AS 12.4% (67)",
    whyHit:
      "Large affected-student pool plus a direct $817K Attendance Support and Interventions action. Also has CWA procedure training, SART implementation, and mentorship language.",
    evidence: 'Action 2A.3 names "Attendance Support and Interventions."',
    lcapUrl: "https://cdeunifiedstoragewest.blob.core.windows.net/lcaps/78a3dcf1-cdf8-4cfb-a38b-d5722a1df60d.pdf",
    superintendent: ["Miki Inbody, Superintendent", "miki.inbody@fusd.net", "(909) 357-7600 Ext. 29109"],
    businessContact: [
      "Leslie Barnes, Associate Superintendent, Business Services",
      "leslie.barnes@fusd.net",
      "(909) 357-7600 Ext. 29117"
    ]
  },
  {
    priority: 5,
    cdsCode: "36750440000000",
    district: "Hesperia Unified",
    absenteeism: "33.5% chronic absenteeism; 5,611 chronically absent students; worsening by 1.6 points.",
    riskRed:
      "SED 36.4% (4,809); HI 33.8% (4,380); SWD 39.4% (1,095); EL 31.8% (914); WH 26.9% (575); AA 44.3% (464); HOM 39.0% (460); LTEL 32.2% (232); MR 35.4% (131); FOS 33.0% (112); PI 47.2% (17)",
    riskOrange: "AS 16.1% (18); FI 19.5% (8)",
    whyHit:
      "Big, worsening need and multiple attendance-adjacent actions, including $2.85M Outreach and broader actions tied to attendance, transportation, and social-emotional support.",
    evidence: 'Action 2.11 is titled "Outreach" and supports school-site protocols across 25 sites.',
    lcapUrl: "https://cdeunifiedstoragewest.blob.core.windows.net/lcaps/54c6e10b-5cbb-48eb-8527-1d19aa9d89d6.pdf",
    superintendent: ["Dr. Michelle Smith, Superintendent", "michelle.smith@hesperiausd.org", "(760) 244-4411 Ext. 7215"],
    businessContact: ["Candace Reines, Chief Business Officer", "candace.reines@hesperiausd.org", "(760) 244-4411 Ext. 7238"]
  },
  {
    priority: 6,
    cdsCode: "01612590000000",
    district: "Oakland Unified",
    absenteeism: "27.9% chronic absenteeism; 7,062 chronically absent students; 2 red and 3 orange indicators.",
    riskRed: "LTEL 37.5% (338); PI 58.2% (110)",
    riskOrange: "AA 43.5% (2,003); FOS 45.9% (56); AI 38.0% (27)",
    whyHit:
      "Huge operational need and a $10.86M Attendance Supports action. Strong fit for outreach automation tied to site attendance teams, case management, and family communication.",
    evidence: 'Action 3.5 says OUSD will "improve attendance and reduce chronic absence."',
    lcapUrl: "https://cdeunifiedstoragewest.blob.core.windows.net/lcaps/b3b05a65-aef4-48e9-8bc9-d1063660a0ba.pdf",
    superintendent: ["Dr. Denise Saddler, Interim Superintendent", "denise.saddler@ousd.org"],
    businessContact: [
      "Tara Gard, Deputy Superintendent of Business and Operations",
      "tara.gard@ousd.org",
      "(510) 879-8000 Ext. 52542"
    ]
  },
  {
    priority: 7,
    cdsCode: "38684780000000",
    district: "San Francisco Unified",
    absenteeism: "21.6% chronic absenteeism; 7,461 chronically absent students; worsening by 1.4 points.",
    riskRed:
      "SED 29.7% (5,426); HI 34.3% (3,837); EL 25.2% (2,526); SWD 34.2% (2,058); AA 52.7% (1,126); MR 20.1% (890); HOM 41.4% (732); LTEL 28.6% (244); FI 20.6% (230); PI 60.4% (137); FOS 60.0% (108); AI 75.4% (46)",
    riskOrange: "WH 12.4% (618)",
    whyHit:
      "Strategic-logo target with the largest affected count in this list and $6.48M in strict attendance actions. Longer sales cycle, but the need and budget are real.",
    evidence: 'LCAP actions include "Safe & Supportive Schools: Improve Attendance" and "Reduce Chronic Absenteeism."',
    lcapUrl: "https://cdeunifiedstoragewest.blob.core.windows.net/lcaps/1619012f-08ab-405e-b611-2ee343120761.pdf",
    superintendent: ["Dr. Maria Su, Superintendent", "mariasu@sfusd.edu", "(415) 241-6121"],
    businessContact: [
      "Chris Mount-Benites, Deputy Superintendent of Business Services",
      "mount-benitesc@sfusd.edu",
      "(415) 241-6542"
    ]
  },
  {
    priority: 8,
    cdsCode: "33672150000000",
    district: "Riverside Unified",
    absenteeism: "18.0% chronic absenteeism; 4,873 chronically absent students; 2 red and 8 orange indicators.",
    riskRed: "FOS 27.6% (62); AI 32.9% (24)",
    riskOrange:
      "SED 20.9% (4,251); SWD 23.7% (1,106); WH 13.3% (524); HOM 29.4% (493); AA 22.3% (327); MR 16.7% (173); FI 8.9% (24); PI 27.4% (23)",
    whyHit:
      "Enterprise-fit district with almost $19M across attendance case management actions, schoolwide attendance plans, home visits, and family barriers work.",
    evidence: 'Action 3.2b says teams will "Develop and implement school-wide attendance plans."',
    lcapUrl: "https://cdeunifiedstoragewest.blob.core.windows.net/lcaps/1204e03f-c93b-4249-9283-c5a969b5a27a.pdf",
    superintendent: ["Dr. Sonia Llamas, Superintendent of Schools", "sllamas@riversideunified.org", "(951) 788-7135 Ext. 80401"],
    businessContact: [
      "Erin Power, Assistant Superintendent, Business Services",
      "epower@riversideunified.org",
      "(951) 788-7135 Ext. 80423"
    ]
  },
  {
    priority: 9,
    cdsCode: "37683790000000",
    district: "San Ysidro Elementary",
    absenteeism: "28.7% chronic absenteeism; 1,271 chronically absent students; worsening; 7 red indicators.",
    riskRed: "HI 30.3% (1,171); SED 31.6% (1,139); EL 34.6% (757); HOM 48.9% (429); SWD 36.3% (302); WH 36.1% (30); FOS 55.6% (10)",
    riskOrange: "LTEL 36.8% (91); AA 19.9% (28); FI 10.5% (14); AS 14.7% (5)",
    whyHit:
      "High-intent pilot target: $641K Attendance Initiative Support for outreach consultants, attendance recovery, family assistance, and SART.",
    evidence: 'Action 2.6 includes "family assistance services, and Student Attendance Review Teams."',
    lcapUrl: "https://cdeunifiedstoragewest.blob.core.windows.net/lcaps/cd6fa411-2f60-4bd8-91e4-efa07a2105ea.pdf",
    superintendent: ["Dr. Gina Potter, Superintendent", "gina.potter@sysdschools.org", "(619) 428-4476 Ext. 3021"],
    businessContact: ["Marilyn Adrianzen, Chief Business Official", "marilyn.adrianzen@sysdschools.org", "(619) 428-4476 Ext. 3004"]
  },
  {
    priority: 10,
    cdsCode: "39685930000000",
    district: "Manteca Unified",
    absenteeism: "23.0% chronic absenteeism; 4,198 chronically absent students; worsening; 9 red indicators.",
    riskRed:
      "SED 25.1% (3,276); HI 25.2% (2,497); SWD 30.2% (765); WH 22.8% (542); HOM 33.5% (484); AA 29.2% (298); PI 31.5% (64); FOS 34.9% (37); AI 37.5% (24)",
    riskOrange: "EL 21.1% (1,001); MR 23.5% (167); LTEL 19.6% (155); FI 11.3% (104)",
    whyHit:
      "Large district with $22.25M for connectedness/attendance/behavior and a specific chronic absenteeism action using parent contact, student messaging, absence review, and COST teams.",
    evidence: 'French Camp action says the committee will "contact parents, message students."',
    lcapUrl: "https://cdeunifiedstoragewest.blob.core.windows.net/lcaps/4c25a239-3d83-495b-be8a-8e156fea0ca0.pdf",
    superintendent: ["Clark Burke, Superintendent", "cburke@musd.net", "(209) 825-3200"],
    businessContact: [
      "Victoria Brunn, Chief Business and Information Officer, Business Services",
      "vbrunn@musd.net",
      "(209) 858-0728"
    ]
  },
  {
    priority: 11,
    cdsCode: "50712170000000",
    district: "Patterson Joint Unified",
    absenteeism: "20.2% chronic absenteeism; 860 chronically absent students; worsening by 1.4 points.",
    riskRed: "SED 22.5% (766); HI 20.1% (662); SWD 27.8% (209); HOM 46.8% (74); MR 29.6% (64); LTEL 21.1% (57); AA 22.0% (36); PI 26.7% (16); FOS 43.5% (10)",
    riskOrange: "EL 18.8% (285)",
    whyHit:
      "Strong workflow fit: MTSS for behavior/SEL/attendance, robust attendance planning, targeted interventions, and clear Tier 1/Tier 2/Tier 3 framing.",
    evidence: 'Action 6.3 says "Develop a robust attendance plan."',
    lcapUrl: "https://cdeunifiedstoragewest.blob.core.windows.net/lcaps/aeb27daf-e497-4c97-bcc2-4bf535841e13.pdf",
    superintendent: ["Dr. Reyes Gauna, Superintendent", "rgauna@patterson.k12.ca.us", "(209) 895-7700 Ext. 20280"],
    businessContact: ["Sandra Madera, Executive Director of Fiscal Services", "smadera@patterson.k12.ca.us", "(209) 895-7700 Ext. 20235"]
  },
  {
    priority: 12,
    cdsCode: "19649800000000",
    district: "Santa Monica-Malibu Unified",
    absenteeism: "13.6% chronic absenteeism; 783 chronically absent students; worsening by 0.2 points.",
    riskRed: "SED 22.3% (406); SWD 20.9% (219); AA 22.3% (80)",
    riskOrange: "WH 11.0% (318); HI 18.8% (306); EL 17.2% (78); MR 10.9% (39); AS 6.9% (33); HOM 33.0% (32); LTEL 21.3% (13)",
    whyHit:
      "Smaller volume, but extremely explicit outbound-call fit: outreach calls, unverified absence calls, SART/SARB conferences, digital monitoring, and Student Outreach Specialists.",
    evidence: 'Action 3.4 says specialists will "make outreach calls to students\' homes."',
    lcapUrl: "https://cdeunifiedstoragewest.blob.core.windows.net/lcaps/a3ff706d-fee9-436f-9c2e-d4151c2b8736.pdf",
    superintendent: ["Dr. Antonio Shelton, Superintendent", "ashelton@smmusd.org", "(310) 450-8338 Ext. 70229"],
    businessContact: [
      "Melody Canady, Assistant Superintendent, Business and Fiscal Services",
      "mcanady@smmusd.org",
      "(310) 450-8338 Ext. 70269"
    ]
  },
  {
    priority: 13,
    cdsCode: "19645680000000",
    district: "Glendale Unified",
    absenteeism: "11.3% chronic absenteeism; 2,072 chronically absent students; 1 red and 8 yellow indicators.",
    riskRed: "FOS 36.6% (15)",
    riskOrange: "none",
    whyHit:
      "Lower rate than others, but one of the clearest phone-call workflows: weekly parent calls, truancy notices, SART/SARB escalation, Q At-Risk Dashboard, bilingual communication, and $1.49M Student Attendance Support.",
    evidence: 'Support section: "Weekly phone calls will be made to parents."',
    lcapUrl: "https://cdeunifiedstoragewest.blob.core.windows.net/lcaps/40285777-a780-4ba2-879d-cb46ed3ae76f.pdf",
    superintendent: ["Dr. Darneika Watson, Superintendent", "dwatson@gusd.net", "(818) 241-3111"],
    businessContact: ["William Young, Assistant Superintendent, Business Services", "williamyoung@gusd.net", "(818) 241-3111 Ext. 1271"]
  },
  {
    priority: 14,
    cdsCode: "33752420000000",
    district: "Val Verde Unified",
    absenteeism: "24.1% chronic absenteeism; 3,170 chronically absent students; worsening by 0.2 points.",
    riskRed: "SED 25.4% (2,969); HI 24.3% (2,582); SWD 31.9% (685); AA 25.9% (339); HOM 35.7% (289); LTEL 29.5% (176); MR 30.8% (124); FOS 32.8% (60)",
    riskOrange: "EL 21.0% (711); AS 11.3% (22)",
    whyHit:
      "Large need and $18.54M in broad attendance-related actions. Best angle is helping resource-center and MTSS teams create consistent family touchpoints around attendance barriers.",
    evidence: 'Action 3.2 says supports address barriers "to improve attendance."',
    lcapUrl: "https://cdeunifiedstoragewest.blob.core.windows.net/lcaps/ef041bb6-5491-42b6-a22e-d2dc638e8998.pdf",
    superintendent: ["Dr. Gordon Amerson, Superintendent", "gamerson@valverde.edu", "(951) 940-6100 Ext. 10201"],
    businessContact: ["Garrick Owen, Assistant Superintendent", "gowen@valverde.edu", "(951) 940-6100 Ext. 10601"]
  },
  {
    priority: 15,
    cdsCode: "24657710000000",
    district: "Merced City Elementary",
    absenteeism: "23.2% chronic absenteeism; 2,762 chronically absent students; worsening by 1.4 points.",
    riskRed:
      "SED 25.1% (2,556); HI 23.5% (1,957); SWD 34.0% (561); EL 20.1% (419); WH 22.3% (283); AA 43.6% (267); MR 22.6% (125); HOM 63.0% (114); LTEL 24.1% (78); AI 25.5% (13)",
    riskOrange: "AS 10.2% (103); FOS 24.0% (44)",
    whyHit:
      "Good mid-market scale and multiple attendance-linked actions. Best angle is outbound calls as a capacity multiplier for wellness, engagement, and attendance teams.",
    evidence: 'Student Wellness action targets "attendance, and engagement in learning."',
    lcapUrl: "https://cdeunifiedstoragewest.blob.core.windows.net/lcaps/4098b9b1-72c9-4925-ba33-233a958d377c.pdf",
    superintendent: ["Julianna Stocking, Superintendent", "jstocking@mcsd.k12.ca.us", "(209) 385-6640"],
    businessContact: ["Linda Parker, Chief Business Officer", "leparker@mcsd.k12.ca.us", "(209) 385-6643"]
  },
  {
    priority: 16,
    cdsCode: "50712900000000",
    district: "Sylvan Union Elementary",
    absenteeism: "15.1% chronic absenteeism; 1,257 chronically absent students; worsening by 0.4 points.",
    riskRed: "SWD 21.6% (295); AA 25.8% (64); LTEL 20.8% (49); FOS 23.5% (4)",
    riskOrange: "SED 17.2% (1,004); HI 17.7% (757); WH 12.1% (245); EL 13.6% (185); PI 14.4% (15); FI 6.4% (13); AI 16.7% (6)",
    whyHit:
      "Strong attendance-operations fit: $442K Pupil Attendance action, attendance specialists, Child Welfare Liaison meetings, and STATS/data-driven intervention language.",
    evidence: 'Action 1.4 says students will be "monitored for positive attendance."',
    lcapUrl: "https://cdeunifiedstoragewest.blob.core.windows.net/lcaps/f22dc06d-07f7-47b5-b7d2-d85accabf965.pdf",
    superintendent: ["Diolinda Peterson, Superintendent", "dpeterson@sylvan.k12.ca.us", "(209) 574-5000 Ext. 1200"],
    businessContact: ["Lizett Aguilar, Assistant Superintendent, Business Services", "laguilar@sylvan.k12.ca.us", "(209) 574-5000 Ext. 210"]
  },
  {
    priority: 17,
    cdsCode: "27659610000000",
    district: "Alisal Union",
    absenteeism: "16.0% chronic absenteeism; 1,207 chronically absent students; worsening by 1.3 points.",
    riskRed: "SWD 24.4% (277); WH 22.0% (13)",
    riskOrange: "HI 15.9% (1,164); SED 16.2% (1,133); EL 13.8% (587); HOM 14.7% (252); FI 19.0% (12); MR 20.0% (8)",
    whyHit:
      "Site-based attendance incentive plans across schools, parent outreach, and peer mentorship. Good fit for multilingual voice nudges and campaign tracking.",
    evidence: 'Action 3.21 includes "parent outreach, and peer mentorship efforts."',
    lcapUrl: "https://cdeunifiedstoragewest.blob.core.windows.net/lcaps/27958dc2-d1aa-45bd-a886-674af9ec4e67.pdf",
    superintendent: ["Monica Anzo, Superintendent", "monica.anzo@alisal.org", "(831) 753-5700 Ext. 2014"],
    businessContact: ["Rais Abbasi, Associate Superintendent", "rais.abbasi@alisal.org", "(831) 753-5700 Ext. 2033"]
  },
  {
    priority: 18,
    cdsCode: "42767860000000",
    district: "Santa Barbara Unified",
    absenteeism: "17.9% chronic absenteeism; 1,059 chronically absent students; worsening by 1.1 points.",
    riskRed: "SED 21.9% (854); HI 20.6% (799); SWD 25.5% (250); LTEL 25.8% (33)",
    riskOrange: "EL 18.6% (235); WH 12.2% (180); HOM 22.5% (134); AS 7.5% (8)",
    whyHit:
      "Strong family-engagement wedge: $1.60M for Family Engagement Liaisons who support student success and remove attendance barriers. Good buyer if positioning is family engagement + attendance recovery.",
    evidence: 'Action 3.2 names "removing barriers to attendance and wellness."',
    lcapUrl: "https://cdeunifiedstoragewest.blob.core.windows.net/lcaps/d6ee1e00-8b23-4418-9951-3fc36400dd14.pdf",
    superintendent: ["Hilda Maldonado, Superintendent", "hmaldonado@sbunified.org", "(805) 963-4338 Ext. 6201"],
    businessContact: ["Conrad Tedeschi, Assistant Superintendent of Business Services", "ctedeschi@sbunified.org", "(805) 963-4338 Ext. 6230"]
  },
  {
    priority: 19,
    cdsCode: "33671160000000",
    district: "Menifee Union",
    absenteeism: "11.5% chronic absenteeism; 1,480 chronically absent students; 1 red and 1 orange indicator.",
    riskRed: "PI 26.4% (14)",
    riskOrange: "FOS 22.4% (26)",
    whyHit:
      "Improving overall, but very specific operational fit: $720K Attendance Intervention and Support, attendance monitoring, root-cause analysis, SART/SARB/DA mediation, liaisons, outreach logs, and re-entry meetings.",
    evidence: 'Action 1.3 tracks "family outreach logs and resolution of identified attendance barriers."',
    lcapUrl: "https://cdeunifiedstoragewest.blob.core.windows.net/lcaps/2a1c1c70-7e1f-4ee5-80f0-e88f71ec90d7.pdf",
    superintendent: ["Dr. Jennifer Root, Superintendent", "jennifer.root@menifeeusd.org", "(951) 672-1851 Ext. 49600"],
    businessContact: ["Marc Bommarito, Assistant Superintendent Business Services", "marc.bommarito@menifeeusd.org", "(951) 672-1851 Ext. 49120"]
  },
  {
    priority: 20,
    cdsCode: "34673140000000",
    district: "Elk Grove Unified",
    absenteeism: "20.1% chronic absenteeism; 9,013 chronically absent students; 3 red and 8 orange indicators.",
    riskRed: "PI 35.4% (310); FOS 32.5% (51); AI 38.2% (39)",
    riskOrange:
      "SED 25.1% (7,075); HI 26.9% (3,350); SWD 29.9% (2,248); EL 20.3% (2,015); AA 30.5% (1,330); MR 20.6% (850); HOM 50.9% (513); LTEL 20.4% (262)",
    whyHit:
      "Largest affected-student count here and $1.85M Family & Community Engagement and Attendance Improvement Program. Likely heavier procurement, but strategically important.",
    evidence: 'Action 4.2 supports "schools, students, and parents" around regular attendance.',
    lcapUrl: "https://cdeunifiedstoragewest.blob.core.windows.net/lcaps/68420868-bbd0-45f1-82c2-3bba3c209ffe.pdf",
    superintendent: ["David Reilly, Superintendent", "dereilly@egusd.net", "(916) 686-7700"],
    businessContact: ["Amari Watkins, Chief Financial Officer", "amawatki@egusd.net", "(916) 686-7769"]
  }
];

const fastestPilotTargets = "Vallejo, Mojave, Snowline, San Ysidro, Santa Monica-Malibu, Glendale, Patterson, Sylvan.";
const strategicLogoTargets = "Oakland, San Francisco, Hesperia, Fontana, Riverside, Manteca, Elk Grove";

function dashboardUrl(cdsCode: string) {
  return `https://www.caschooldashboard.org/reports/${cdsCode}/2024/academic-engagement#chronic-absenteeism`;
}

function directoryUrl(cdsCode: string) {
  return `https://www.cde.ca.gov/SchoolDirectory/details?cdscode=${cdsCode}`;
}

export default function ChronicAbsenteeismReportPage() {
  return (
    <main className="report-shell">
      <section className="report-hero">
        <Link className="back-link" href="/">
          <ArrowLeft size={17} />
          Home
        </Link>
        <p className="eyebrow">GTM report</p>
        <h1>Best districts to market chronic absenteeism outbound voice calls</h1>
        <p className="report-summary">
          Rerun on May 16, 2026 using the ca-lcap MCP. I weighted chronic absenteeism severity, affected-student scale,
          LCAP-funded attendance actions, explicit family outreach or phone-call workflow, and sales practicality. I
          excluded tiny county-office or one-school outliers when the MCP score was driven mostly by very small enrollment.
        </p>
      </section>

      <section className="report-notes" aria-label="Report notes">
        <p>
          LCAP PDF links, Dashboard outcomes, student-group outcomes, LCAP action evidence, and contacts are from the
          ca-lcap MCP. Dashboard links go to each district&apos;s 2024 CA School Dashboard Academic Engagement page.
        </p>
        <p>
          <strong>Dashboard color note:</strong> red/orange counts are the number of student groups that scored Red or
          Orange on the CA School Dashboard chronic absenteeism indicator. Red is the lowest performance color; Orange is
          the next-lowest. The student-group detail below expands those color counts as group chronic absenteeism rate
          (chronically absent students), excluding the all-students row and privately suppressed groups.
        </p>
        <p>
          <strong>Student group abbreviations:</strong> AA=African American, AI=American Indian, AS=Asian, EL=English
          Learners, FI=Filipino, FOS=Foster Youth, HI=Hispanic, HOM=Homeless, LTEL=Long-Term English Learners, MR=Two or
          More Races, PI=Pacific Islander, SED=Socioeconomically Disadvantaged, SWD=Students with Disabilities, WH=White.
        </p>
      </section>

      <section className="target-grid" aria-label="Target groups">
        <div>
          <span>Fastest pilot targets</span>
          <strong>{fastestPilotTargets}</strong>
        </div>
        <div>
          <span>Strategic logo targets</span>
          <strong>{strategicLogoTargets}. Higher need and budget, but heavier procurement.</strong>
        </div>
      </section>

      <section className="report-table-section">
        <div className="table-scroll">
          <table className="report-table">
            <thead>
              <tr>
                <th>Priority</th>
                <th>District</th>
                <th>Absenteeism / Scale</th>
                <th>Student Group Risk Detail</th>
                <th>Why Hit</th>
                <th>LCAP Evidence</th>
                <th>Dashboard Data</th>
                <th>LCAP URL</th>
                <th>Superintendent</th>
                <th>Business / Finance Contact</th>
                <th>Contact Source</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => (
                <tr key={row.priority}>
                  <td className="priority-cell">{row.priority}</td>
                  <td className="district-cell">{row.district}</td>
                  <td>{row.absenteeism}</td>
                  <td>
                    <RiskLines red={row.riskRed} orange={row.riskOrange} />
                  </td>
                  <td>{row.whyHit}</td>
                  <td>{row.evidence}</td>
                  <td className="source-cell">
                    <a href={dashboardUrl(row.cdsCode)} target="_blank" rel="noreferrer">
                      Dashboard
                      <ExternalLink size={14} />
                    </a>
                  </td>
                  <td className="source-cell">
                    <a href={row.lcapUrl} target="_blank" rel="noreferrer">
                      PDF
                      <ExternalLink size={14} />
                    </a>
                  </td>
                  <td>
                    <ContactLines lines={row.superintendent} />
                  </td>
                  <td>
                    <ContactLines lines={row.businessContact} />
                  </td>
                  <td className="source-cell">
                    <a href={directoryUrl(row.cdsCode)} target="_blank" rel="noreferrer">
                      CDE Directory
                      <ExternalLink size={14} />
                    </a>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </main>
  );
}

function RiskLines({ red, orange }: { red: string; orange: string }) {
  return (
    <span className="risk-lines">
      <span>
        <strong>Red:</strong> {red}
      </span>
      <span>
        <strong>Orange:</strong> {orange}
      </span>
    </span>
  );
}

function ContactLines({ lines }: { lines: string[] }) {
  return (
    <span className="contact-lines">
      {lines.map((line) => (
        <span key={line}>{line}</span>
      ))}
    </span>
  );
}

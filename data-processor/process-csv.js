const fs = require('fs').promises;
const csv = require('csv-parser');
const createReadStream = require('fs').createReadStream;

/**
 * 滞在時間文字列を分に変換
 * @param {string} timeStr - "01:23:45" 形式
 * @returns {number} 分数
 */
function parseStayTime(timeStr) {
    const [hours, minutes, seconds] = timeStr.split(':').map(Number);
    return hours * 60 + minutes + seconds / 60;
}

/**
 * 日付文字列をDateオブジェクトに変換
 * @param {string} dateStr - "2024-03-29 23:33:42 +0900" 形式
 * @returns {Date}
 */
function parseDateTime(dateStr) {
    return new Date(dateStr);
}

/**
 * 年月文字列を生成
 * @param {Date} date
 * @returns {string} "2024-03" 形式
 */
function getYearMonth(date) {
    return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
}

/**
 * 期間フィルタリング（指定月数前から現在まで）
 * @param {Date} date
 * @param {number} months - 何ヶ月前まで
 * @returns {boolean}
 */
function isWithinPeriod(date, months) {
    const cutoffDate = new Date();
    cutoffDate.setMonth(cutoffDate.getMonth() - months);
    return date >= cutoffDate;
}

/**
 * 7月のデータのみかどうかを判定
 * @param {Date} date
 * @returns {boolean}
 */
function isJulyOnly(date) {
    const yearMonth = getYearMonth(date);
    return yearMonth === '2025-07';
}

/**
 * 8月のデータのみかどうかを判定
 * @param {Date} date
 * @returns {boolean}
 */
function isAugustOnly(date) {
    const yearMonth = getYearMonth(date);
    return yearMonth === '2025-08';
}

/**
 * CSVデータを読み込んでユーザー統計を生成
 */
async function processCSV() {
    const users = {};
    const records = [];

    console.log('📊 CSVファイルを読み込み中...');

    // CSVファイルを読み込み
    return new Promise((resolve, reject) => {
        createReadStream('nihouse.csv')
            .pipe(csv())
            .on('data', (row) => {
                // データクリーニング
                const customerName = row['顧客名']?.trim();
                const checkinTime = row['チェックイン日時']?.trim();
                const stayTime = row['滞在時間']?.trim();

                if (!customerName || !checkinTime || !stayTime) {
                    console.warn('⚠️  不正なデータをスキップ:', row);
                    return;
                }

                try {
                    const checkinDate = parseDateTime(checkinTime);
                    const stayMinutes = parseStayTime(stayTime);

                    if (stayMinutes <= 0) {
                        console.warn('⚠️  滞在時間が0以下のデータをスキップ:', row);
                        return;
                    }

                    records.push({
                        customerName,
                        checkinDate,
                        stayMinutes,
                        yearMonth: getYearMonth(checkinDate)
                    });
                } catch (error) {
                    console.warn('⚠️  データ解析エラー:', row, error.message);
                }
            })
            .on('end', () => {
                console.log(`✅ ${records.length}件のレコードを処理しました`);

                // 統計計算
                const sixMonthStats = calculateUserStats(records, 6);
                const julyStats = calculateUserStats(records, 'july');
                const augustStats = calculateUserStats(records, 'august');

                const result = {
                    sixMonthData: sixMonthStats,
                    julyData: julyStats,
                    augustData: augustStats,
                    metadata: {
                        totalRecords: records.length,
                        uniqueUsers: Object.keys(users).length,
                        generatedAt: new Date().toISOString(),
                        periodSixMonths: '直近6ヶ月',
                        periodJuly: '2025年7月',
                        periodAugust: '2025年8月'
                    }
                };

                resolve(result);
            })
            .on('error', reject);
    });
}

/**
 * ユーザー統計を計算
 * @param {Array} records - 全レコード
 * @param {number|string} period - 対象期間（6なら6ヶ月間、'july'なら7月、'august'なら8月）
 * @returns {Array} ユーザー統計配列
 */
function calculateUserStats(records, period) {
    const userStats = {};
    const userFirstCheckIn = {};

    let periodName;
    let periodLength; // 月平均計算用の期間長

    if (period === 'july') {
        periodName = '7月';
        periodLength = 1;
    } else if (period === 'august') {
        periodName = '8月';
        periodLength = 1;
    } else {
        periodName = `${period}ヶ月間`;
        periodLength = period;
    }

    console.log(`📈 ${periodName}のユーザー統計を計算中...`);

    // 全レコードから各ユーザーの最初のチェックイン日を記録
    records.forEach(record => {
        const { customerName, checkinDate } = record;
        if (!userFirstCheckIn[customerName] || checkinDate < userFirstCheckIn[customerName]) {
            userFirstCheckIn[customerName] = checkinDate;
        }
    });

    // 期間内のレコードのみフィルタ
    const filteredRecords = records.filter(record => {
        if (period === 'july') {
            return isJulyOnly(record.checkinDate);
        } else if (period === 'august') {
            return isAugustOnly(record.checkinDate);
        } else {
            // 6ヶ月間の場合は従来通り
            return isWithinPeriod(record.checkinDate, period);
        }
    });

    console.log(`📅 ${periodName}で${filteredRecords.length}件のレコードを対象`);

    // ユーザー×年月でグループ化
    filteredRecords.forEach(record => {
        const { customerName, yearMonth, stayMinutes } = record;

        if (!userStats[customerName]) {
            userStats[customerName] = {};
        }

        if (!userStats[customerName][yearMonth]) {
            userStats[customerName][yearMonth] = {
                count: 0,
                totalMinutes: 0
            };
        }

        userStats[customerName][yearMonth].count++;
        userStats[customerName][yearMonth].totalMinutes += stayMinutes;
    });

    // 月平均を計算
    const result = [];
    for (const userName in userStats) {
        const monthlyData = userStats[userName];
        
        let totalVisits = 0;
        let totalMinutes = 0;
        const activeMonths = Object.keys(monthlyData).length;

        for (const yearMonth in monthlyData) {
            totalVisits += monthlyData[yearMonth].count;
            totalMinutes += monthlyData[yearMonth].totalMinutes;
        }

        // 月平均計算（活動していない月は0として扱う）
        const monthlyVisits = totalVisits / periodLength;
        const monthlyHours = totalMinutes / (60 * periodLength);

        result.push({
            name: userName,
            monthlyVisits: Math.round(monthlyVisits * 10) / 10, // 小数点1桁
            monthlyHours: Math.round(monthlyHours * 10) / 10,   // 小数点1桁
            activeMonths,
            totalVisits,
            totalHours: Math.round(totalMinutes / 60 * 10) / 10,
            firstCheckIn: userFirstCheckIn[userName].toISOString().split('T')[0] // YYYY-MM-DD形式
        });
    }

    // 月平均利用回数で降順ソート
    result.sort((a, b) => b.monthlyVisits - a.monthlyVisits);

    console.log(`✅ ${result.length}名のユーザー統計を生成`);
    console.log(`📊 トップユーザー: ${result[0]?.name} (${result[0]?.monthlyVisits}回/月)`);

    return result;
}

/**
 * メイン実行関数
 */
async function main() {
    try {
        console.log('🚀 NIHO利用データ分析を開始...');
        
        const statistics = await processCSV();
        
        // JSON出力
        const outputPath = '../docs/user-data.json';
        await fs.writeFile(outputPath, JSON.stringify(statistics, null, 2));
        
        console.log('📄 分析結果をJSONに出力:', outputPath);
        console.log('📊 統計サマリー:');
        console.log(`   - 6ヶ月間ユーザー数: ${statistics.sixMonthData.length}名`);
        console.log(`   - 7月ユーザー数: ${statistics.julyData.length}名`);
        console.log(`   - 8月ユーザー数: ${statistics.augustData.length}名`);
        console.log(`   - 総レコード数: ${statistics.metadata.totalRecords}件`);
        
        // トップ5ユーザーを表示
        console.log('\n🏆 6ヶ月間 トップ5ユーザー:');
        statistics.sixMonthData.slice(0, 5).forEach((user, index) => {
            console.log(`   ${index + 1}. ${user.name}: ${user.monthlyVisits}回/月, ${user.monthlyHours}時間/月`);
        });

        console.log('\n🏆 7月 トップ5ユーザー:');
        statistics.julyData.slice(0, 5).forEach((user, index) => {
            console.log(`   ${index + 1}. ${user.name}: ${user.monthlyVisits}回/月, ${user.monthlyHours}時間/月`);
        });

        console.log('\n🏆 8月 トップ5ユーザー:');
        statistics.augustData.slice(0, 5).forEach((user, index) => {
            console.log(`   ${index + 1}. ${user.name}: ${user.monthlyVisits}回/月, ${user.monthlyHours}時間/月`);
        });

        console.log('\n✅ 処理完了! ダッシュボードで確認してください。');
        
    } catch (error) {
        console.error('❌ エラーが発生しました:', error);
        process.exit(1);
    }
}

// スクリプト実行
if (require.main === module) {
    main();
}

module.exports = { processCSV, parseStayTime, parseDateTime };
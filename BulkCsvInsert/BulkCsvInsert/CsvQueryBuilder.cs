using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace BulkCsvInsert
{
    public static class CsvQueryBuilder
    {
        public static string BaseInsert { get; } = @"INSERT INTO `hc_leads`.`adwords_geo_target`
                                    (`id`,
                                    `adwords_campaign`,
                                    `adwords_adgroup`,
                                    `country`,
                                    `region`,
                                    `city`,
                                    `clics`,
                                    `impressions`,
                                    `ctr`,
                                    `cpc`,
                                    `cost`,
                                    `avg_position`,
                                    `conversions`,
                                    `cost_conversion`,
                                    `avg_conversion`,
                                    `all_conversions`,
                                    `conversions_per_impression`) VALUES";

        public static string GenerateInsert(IEnumerable<string> lines)
        {
            var queryBuilder = new StringBuilder(BaseInsert);

            foreach (var data in lines.Select(line => line.Split(',')))
            {
                var fixedSplitLength = data.Length == 17;
                queryBuilder.Append($@"[({data[0]},
                                        '{data[1]}',
                                        '{(fixedSplitLength ? data[2]:data[2] + data[3]).Replace("'", "\\'")}',
                                        '{(fixedSplitLength ? data[3]:data[4]).Replace("'", "\\'")}',
                                        '{(fixedSplitLength ? data[4] : data[5]).Replace("'", "\\'")}',
                                        '{(fixedSplitLength ? data[5] : data[6]).Replace("'","\\'")}',
                                        {(fixedSplitLength ? data[6] : data[7])},
                                        {(fixedSplitLength ? data[7] : data[8])},
                                        {(fixedSplitLength ? data[8] : data[9])},
                                        {(fixedSplitLength ? data[9] : data[10])},
                                        {(fixedSplitLength ? data[10] : data[11])},
                                        {(fixedSplitLength ? data[11] : data[12])},
                                        {(fixedSplitLength ? data[12] : data[13])},
                                        {(fixedSplitLength ? data[13] : data[14])},
                                        {(fixedSplitLength ? data[14] : data[15])},
                                        {(fixedSplitLength ? data[15] : data[16])},
                                        {(fixedSplitLength ? data[16] : data[17])})]");
            }
            return queryBuilder.Replace("][", ",").Replace("[", "").Replace("]", "").Append(";").ToString();

        } 
    }
}
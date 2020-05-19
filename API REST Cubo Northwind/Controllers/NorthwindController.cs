using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using System.Web.Http.Cors;
using Microsoft.AnalysisServices.AdomdClient;

namespace API_REST_Cubo_Northwind.Controllers
{
        [EnableCors(origins: "*", headers: "*", methods: "*")]
        [RoutePrefix("v1/Analysis/Northwind")]
        public class NorthwindController : ApiController
        {
            [HttpGet]
            [Route("Testing")]
            public HttpResponseMessage Testing()
            {
                return Request.CreateResponse(HttpStatusCode.OK, "Prueba de API Exitosa");
            }




        //Traer Items dependiendo la dimension
        [HttpGet]
        [Route("GetItemsByDimension/{dim}/{order}")]
        public HttpResponseMessage GetItemsByDimension(string dim, string order = "DESC")
        {
            string WITH = @"
                WITH 
                SET [OrderDimension] AS 
                NONEMPTY(
                    ORDER(
                        {0}.CHILDREN,
                        {0}.CURRENTMEMBER.MEMBER_NAME, " + order +
                    @")
                )
            ";

            string COLUMNS = @"
                NON EMPTY
                {
                    [Measures].[Fact Ventas Netas]
                }
                ON COLUMNS,    
            ";

            string ROWS = @"
                NON EMPTY
                {
                    [OrderDimension]
                }
                ON ROWS
            ";

            string CUBO_NAME = "[DWH Northwind]";
            WITH = string.Format(WITH, dim);
            string MDX_QUERY = WITH + @"SELECT " + COLUMNS + ROWS + " FROM " + CUBO_NAME;

            Debug.Write(MDX_QUERY);

            List<string> dimension = new List<string>();

            dynamic result = new
            {
                datosDimension = dimension
            };

            using (AdomdConnection cnn = new AdomdConnection(ConfigurationManager.ConnectionStrings["CuboNorthwind"].ConnectionString))
            {
                cnn.Open();
                using (AdomdCommand cmd = new AdomdCommand(MDX_QUERY, cnn))
                {
                    using (AdomdDataReader dr = cmd.ExecuteReader(CommandBehavior.CloseConnection))
                    {
                        while (dr.Read())
                        {
                            dimension.Add(dr.GetString(0));
                        }
                        dr.Close();
                    }
                }
            }

            return Request.CreateResponse(HttpStatusCode.OK, (object)result);
        }




        //Traer Ventas para llenar Pie Chart
        [HttpPost]
            [Route("GetDataPieByDimension/{dim}/{order}")]
            public HttpResponseMessage GetDataPieByDimension([FromBody] dynamic body, string dim = "Cliente", string order = "DESC")
            {
                string dimensionItemParameter = string.Empty;
                string anioParameter = string.Empty;
                string mesParameter = string.Empty;

                foreach (var item in body.itemdim)
                {
                    dimensionItemParameter += "[Dim " + dim + "].[Dim " + dim + " Nombre].[" + item + "],";
                }
                dimensionItemParameter = dimensionItemParameter.TrimEnd(',');
                dimensionItemParameter = @"{" + dimensionItemParameter + "}";

                foreach (var item in body.anios)
                {
                    anioParameter += "[Dim Tiempo].[Dim Tiempo Año].[" + item + "],";
                }
                anioParameter = anioParameter.TrimEnd(',');
                anioParameter = @"{" + anioParameter + "}";

                foreach (var item in body.meses)
                {
                    mesParameter += "[Dim Tiempo].[Dim Tiempo Mes].[" + item + "],";
                }
                mesParameter = mesParameter.TrimEnd(',');
                mesParameter = @"{" + mesParameter + "}";

                string WITH = "";
                string ROW = "";
                string MEMBER = "";
                string COLUMN = "";
                string CUBO_NAME = "";


                WITH = @"WITH SET [DimSet] AS{ STRTOSET(@dimensionItemParameter) } ";
                if (body.anios.Count == 0 || body.anios == null)
                {
                    if (body.meses.Count == 0 || body.meses == null)
                    {
                        ROW = @" NON EMPTY([DimSet])  ON ROWS ";
                        COLUMN = @"SELECT NONEMPTY ([Measures].[Fact Ventas Netas])ON COLUMNS,";
                    }
                    else
                    {
                        WITH += @" SET [MonthSet] AS { STRTOSET(@mesParameter)}";
                        MEMBER = @"MEMBER [SUMVENTASMES] AS 
                                SUM
                                (
                                    [MonthSet],
		                            [Measures].[Fact Ventas Netas]
                                ) 
	                            MEMBER [SUMVENTASANIOS] AS 
                                SUM
                                (
		                            [Dim Tiempo].[Dim Tiempo Año].CHILDREN,
		                            [SUMVENTASMES]
                                )";
                        COLUMN = @"SELECT NONEMPTY ([SUMVENTASANIOS])ON COLUMNS, ";
                        ROW = @"  NON EMPTY([DimSet])  ON ROWS ";
                    }
                }
                else
                {
                    WITH += @" SET [YearSet] AS { STRTOSET(@anioParameter)}";
                    if (body.meses.Count == 0 || body.meses == null)
                    {
                        MEMBER = @"MEMBER [SUMVENTASMES] AS 
                                SUM
                                (
                                    [Dim Tiempo].[Dim Tiempo Mes].CHILDREN,
                                    [Measures].[Fact Ventas Netas]
                                ) 
	                            MEMBER[SUMVENTASANIOS] AS
                                SUM
                                (
                                    [YearSet],
                                    [SUMVENTASMES]
                                )";
                        COLUMN = @"SELECT NONEMPTY ([SUMVENTASANIOS]) ON COLUMNS, ";
                        ROW = @"  NON EMPTY([DimSet])  ON ROWS ";

                    }
                    else
                    {
                        MEMBER = @"MEMBER [SUMVENTASMES] AS SUM
                            (
     
                                [MonthSet],
		                        [Measures].[Fact Ventas Netas]
      
                            ) 
	                        MEMBER [SUMVENTASANIOS] AS 
                            SUM
                            (
		                        [YearSet],
		                        [SUMVENTASMES]
      
                            )";
                        COLUMN = @"SELECT NONEMPTY ([SUMVENTASANIOS])ON COLUMNS, ";
                        ROW = @"  NON EMPTY([DimSet])  ON ROWS ";
                        WITH += @" SET [MonthSet] AS { STRTOSET(@mesParameter)}";
                    }
                }

                CUBO_NAME = "[DWH Northwind]";
                string mdx = WITH + MEMBER + COLUMN + ROW + " FROM " + CUBO_NAME;
                Debug.Write(mdx);

                List<string> dimension = new List<string>();
                List<decimal> ventas = new List<decimal>();
                List<dynamic> lstTabla = new List<dynamic>();


                dynamic result = new
                {
                    datosDimension = dimension,
                    datosVenta = ventas,
                    datosTabla = lstTabla
                };

                using (AdomdConnection cnn = new AdomdConnection(ConfigurationManager.ConnectionStrings["CuboNorthwind"].ConnectionString))
                {
                    cnn.Open();
                    using (AdomdCommand cmd = new AdomdCommand(mdx, cnn))
                    {
                        cmd.Parameters.Add("dimensionItemParameter", dimensionItemParameter);
                        cmd.Parameters.Add("anioParameter", anioParameter);
                        cmd.Parameters.Add("mesParameter", mesParameter);
                        using (AdomdDataReader dr = cmd.ExecuteReader(CommandBehavior.CloseConnection))
                        {
                            while (dr.Read())
                            {
                                dimension.Add(dr.GetString(0));
                                ventas.Add(Math.Round(dr.GetDecimal(1)));

                                dynamic objTabla = new
                                {
                                    descripcion = dr.GetString(0),
                                    valor = Math.Round(dr.GetDecimal(1))
                                };
                                lstTabla.Add(objTabla);
                            }
                            dr.Close();
                        }
                    }
                }

                return Request.CreateResponse(HttpStatusCode.OK, (object)result);
            }


        //Traer Fecha (Mes y Año) para llenar grafica
        [HttpPost]
        [Route("GetDataGraphByDimension/{dim}/{order}")]
        public HttpResponseMessage GetDataBarByDimension([FromBody] dynamic body, string dim = "Cliente", string order = "DESC")
        {
            string dimensionItemParameter = string.Empty;
            string anioParameter = string.Empty;
            string mesParameter = string.Empty;

            foreach (var item in body.itemdim)
            {
                dimensionItemParameter += "[Dim " + dim + "].[Dim " + dim + " Nombre].[" + item + "],";
            }
            dimensionItemParameter = dimensionItemParameter.TrimEnd(',');
            dimensionItemParameter = @"{" + dimensionItemParameter + "}";

            foreach (var item in body.anios)
            {
                anioParameter += "[Dim Tiempo].[Dim Tiempo Año].[" + item + "],";
            }
            anioParameter = anioParameter.TrimEnd(',');
            anioParameter = @"{" + anioParameter + "}";

            foreach (var item in body.meses)
            {
                mesParameter += "[Dim Tiempo].[Dim Tiempo Mes].[" + item + "],";
            }
            mesParameter = mesParameter.TrimEnd(',');
            mesParameter = @"{" + mesParameter + "}";

            string WITH = "";
            string ROW = "";
            string MEMBER = "";
            string COLUMN = "";
            string CUBO_NAME = "";

            MEMBER = @"MEMBER  [limpiaNulls] AS COALESCEEMPTY(  ([Measures].[Fact Ventas Netas]), 0 )";
            WITH = @"WITH SET [DimSet] AS{ STRTOSET(@dimensionItemParameter) } ";
            if (body.anios.Count == 0 || body.anios == null)
            {
                if (body.meses.Count == 0 || body.meses == null)
                {
                    ROW = @" NON EMPTY([DimSet])  ON ROWS ";
                    COLUMN = @"SELECT ([limpiaNulls],([YearSet],[MonthSet])) ON COLUMNS,";
                }
                else
                {
                    WITH += @" SET [MonthSet] AS { STRTOSET(@mesParameter)}";
                    COLUMN = @"SELECT ([limpiaNulls],([YearSet],[MonthSet])) ON COLUMNS, ";
                    ROW = @"  NON EMPTY([DimSet])  ON ROWS ";
                }
            }
            else
            {
                WITH += @" SET [YearSet] AS { STRTOSET(@anioParameter)}";
                if (body.meses.Count == 0 || body.meses == null)
                {
                    COLUMN = @"SELECT ([limpiaNulls],([YearSet],[MonthSet])) ON COLUMNS, ";
                    ROW = @"  NON EMPTY([DimSet])  ON ROWS ";

                }
                else
                {
                    COLUMN = @"SELECT ([limpiaNulls],([YearSet],[MonthSet])) ON COLUMNS, ";
                    ROW = @"  NON EMPTY([DimSet])  ON ROWS ";
                    WITH += @" SET [MonthSet] AS { STRTOSET(@mesParameter)}";
                }
            }

            CUBO_NAME = "[DWH Northwind]";
            string mdx = WITH + MEMBER + COLUMN + ROW + " FROM " + CUBO_NAME;
            Debug.Write(mdx);

            List<string> dimensionVentas = new List<string>();
            List<decimal> datas = new List<decimal>();
            List<dynamic> lstTabla = new List<dynamic>();
            List<string> columns = new List<string>();

            dynamic result = new
            {
                columnas = columns,
                datosTabla = lstTabla
            };

            using (AdomdConnection cnn = new AdomdConnection(ConfigurationManager.ConnectionStrings["CuboNorthwind"].ConnectionString))
            {
                cnn.Open();
                using (AdomdCommand cmd = new AdomdCommand(mdx, cnn))
                {
                    cmd.Parameters.Add("dimensionItemParameter", dimensionItemParameter);
                    cmd.Parameters.Add("anioParameter", anioParameter);
                    cmd.Parameters.Add("mesParameter", mesParameter);
                    using (AdomdDataReader dr = cmd.ExecuteReader(CommandBehavior.CloseConnection))
                    {
                        for (int i = 0; i < body.anios.Count; i++)
                        {
                            foreach (var item in body.meses)
                            {
                                string fechaFormato = item + " " + body.anios[i];
                                columns.Add(fechaFormato);
                            }
                        }
                        int count = dr.FieldCount;
                        while (dr.Read())
                        {
                            datas = new List<decimal>();
                            for (int i = 1; i < count; i++)
                            {
                                datas.Add(Math.Round(dr.GetDecimal(i)));
                            }
                            dynamic objTabla = new
                            {
                                data = datas,
                                label = dr.GetString(0),
                            };
                            lstTabla.Add(objTabla);
                        }
                        dr.Close();
                    }
                }
            }

            return Request.CreateResponse(HttpStatusCode.OK, (object)result);
        }

        // Traer Items de Año
        [HttpGet]
        [Route("GetItemsAnio")]
        public HttpResponseMessage GetItemsAnio()
        {
            string COLUMNS = @"
                NON EMPTY
                {
                    [Measures].[Fact Ventas Netas]
                }
                ON COLUMNS,    
            ";

            string ROWS = @"
                NON EMPTY
                {
                    [Dim Tiempo].[Dim Tiempo Año].CHILDREN
                }
                ON ROWS
            ";

            string CUBO_NAME = "[DWH Northwind]";
            string MDX_QUERY = @"SELECT " + COLUMNS + ROWS + " FROM " + CUBO_NAME;

            Debug.Write(MDX_QUERY);

            List<string> dimension = new List<string>();

            dynamic result = new
            {
                anio = dimension
            };

            using (AdomdConnection cnn = new AdomdConnection(ConfigurationManager.ConnectionStrings["CuboNorthwind"].ConnectionString))
            {
                cnn.Open();
                using (AdomdCommand cmd = new AdomdCommand(MDX_QUERY, cnn))
                {
                    using (AdomdDataReader dr = cmd.ExecuteReader(CommandBehavior.CloseConnection))
                    {
                        while (dr.Read())
                        {
                            dimension.Add(dr.GetString(0));
                        }
                        dr.Close();
                    }
                }
            }

            return Request.CreateResponse(HttpStatusCode.OK, (object)result);
        }

    }
}

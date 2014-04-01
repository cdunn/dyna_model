class Symbol

  DynaModel::Table::COMPARISON_OPERATOR.keys.each do |oper|
    class_eval <<-OPERATORS
      def #{oper}
        "\#\{self.to_s\}.#{oper}"
      end
    OPERATORS
  end

end
